import base64
import copy
import email
import hmac
import re
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Protocol

import inscriptis

################
# Anonymized IDs
################


def anon_id(id_val: str | None, salt: bytes | None) -> str | None:
    """
    Takes the provided FHIR ID and salt and makes a one-way hashed anonymous ID.

    Salt should ideally have length 32 to match the sha256 digest used, and then the result will
    also be exactly 64 characters long (the maximum FHIR ID length).

    If input ID is empty, will return None; if salt is None, will return id_val.
    """
    if not id_val:
        return None
    if not salt:
        return id_val
    return hmac.new(salt, digestmod="sha256", msg=id_val.encode()).hexdigest()


def anon_ref(ref_val: str | None, salt: bytes | None) -> str | None:
    """If input ref is empty or not a ref, will return None; if salt is None, will not anonymize"""
    if not ref_val:
        return None
    try:
        res_type, id_val = ref_val.split("/", 1)
    except ValueError:
        return None
    return f"{res_type}/{anon_id(id_val, salt)}"


######################
# Note text extraction
######################


class RemoteAttachment(ValueError):
    """A note was requested, but it was only available remotely"""


def note_res_has_text(note_res: dict) -> bool:
    """
    Returns whether this note resource has text available.

    i.e. if this returns True, a call to get_text_from_note_res() should work.
    (this is just a little faster because it won't actually process the HTML or text)

    Should not normally raise an exception.
    """
    try:
        attachment = _get_clinical_note_attachment(note_res)
    except ValueError:
        return False

    return attachment.get("data") is not None


def get_text_from_note_res(note_res: dict) -> str:
    """
    Returns the clinical text contained in the given note resource.

    It will try to find the simplest version (plain text) or convert html to plain text if needed.

    Will raise an exception if text cannot be found.
    """
    attachment = _get_clinical_note_attachment(note_res)
    text = _get_note_from_attachment(attachment)

    mimetype, _ = _parse_content_type(attachment["contentType"])
    if mimetype in {"text/html", "application/xhtml+xml"}:
        # An HTML note can confuse/stall NLP.
        # It may include mountains of spans/styling or inline base64 images that aren't relevant
        # to our interests.
        #
        # Inscriptis makes a very readable version of the note, with a focus on maintaining the
        # HTML layout.
        text = inscriptis.get_text(text)

    return text.strip()


def _parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    msg = email.message.EmailMessage()
    msg["content-type"] = content_type
    return msg.get_content_type(), msg.get_content_charset("utf8")


def _mimetype_priority(mimetype: str) -> int:
    """
    Returns priority of mimetypes for docref notes.

    0 means "ignore"
    Higher numbers are higher priority
    """
    if mimetype == "text/plain":
        return 3
    elif mimetype == "text/html":
        return 2
    elif mimetype == "application/xhtml+xml":
        return 1
    return 0


def _get_note_from_attachment(attachment: dict) -> str:
    """
    Decodes a note from an attachment.

    Note that it is assumed a contentType is provided.

    :returns: the attachment's note text
    """
    _mimetype, charset = _parse_content_type(attachment["contentType"])

    if attachment.get("data") is not None:
        return base64.standard_b64decode(attachment["data"]).decode(charset)

    if attachment.get("url") is not None:
        raise RemoteAttachment(
            "Some clinical note texts are only available via URL. "
            "You may want to inline your notes with SMART Fetch."
        )

    # Shouldn't ever get here, because _get_clinical_note_attachment already checks this,
    # but just in case...
    raise ValueError("No data or url field present")  # pragma: no cover


def _get_clinical_note_attachment(resource: dict) -> dict:
    match resource["resourceType"]:
        case "DiagnosticReport":
            attachments = resource.get("presentedForm", [])
        case "DocumentReference":
            attachments = [
                content["attachment"]
                for content in resource.get("content", [])
                if "attachment" in content
            ]
        case _:
            raise ValueError(f"{resource['resourceType']} is not a supported clinical note type.")

    # Find the best attachment to use, based on mimetype.
    # We prefer basic text documents, to avoid confusing NLP with extra formatting (like <body>).
    best_attachment_index = -1
    best_attachment_priority = 0
    for index, attachment in enumerate(attachments):
        if "contentType" in attachment:
            mimetype, _ = _parse_content_type(attachment["contentType"])
            priority = _mimetype_priority(mimetype)
            if priority > best_attachment_priority:
                best_attachment_priority = priority
                best_attachment_index = index

    if best_attachment_index < 0:
        # We didn't find _any_ of our target text content types.
        raise ValueError("No textual mimetype found")

    attachment = attachments[best_attachment_index]

    if attachment.get("data") is None and not attachment.get("url"):
        raise ValueError("No data or url field present")

    return attachments[best_attachment_index]


##########################
# Collection of references
##########################


RefSetData = object | None


class RefSet:  # noqa: PLW1641
    """A class that holds a pile of FHIR resource references, with optional extra data for each"""

    def __init__(self, *others: "str | RefSet"):
        # Maps resource type -> set of IDs (with optional attached data)
        self._ids: dict[str, dict[str, RefSetData]] = {}
        for other in others:
            if isinstance(other, str):
                self.add_ref(other)
            else:
                self.add_set(other)

    def add_id(self, res_type: str, id_val: str, *, data: RefSetData = None) -> None:
        """Will overwrite any existing data for this ID"""
        self._ids.setdefault(res_type, {})[id_val] = data

    def add_ref(self, ref: str, *, data: RefSetData = None) -> None:
        """Will overwrite any existing data for this ref"""
        res_type, id_val = ref.split("/", 1)
        self.add_id(res_type, id_val, data=data)

    def add_set(self, other: "RefSet") -> None:
        for res_type, ids in other._ids.items():
            self._ids.setdefault(res_type, {}).update(copy.deepcopy(ids))

    def get_data_for_id(
        self, res_type: str, id_val: str, *, default: RefSetData = None
    ) -> RefSetData:
        return self._ids.get(res_type, {}).get(id_val, default)

    def get_data_for_ref(self, ref: str, *, default: RefSetData = None) -> RefSetData:
        res_type, id_val = ref.split("/", 1)
        return self.get_data_for_id(res_type, id_val, default=default)

    def has_id(self, res_type: str, id_val: str) -> bool:
        return id_val in self._ids.get(res_type, {})

    def has_ref(self, ref: str) -> bool:
        res_type, id_val = ref.split("/", 1)
        return self.has_id(res_type, id_val)

    def has_type(self, res_type: str) -> bool:
        return res_type in self._ids

    def __bool__(self) -> bool:
        return bool(self._ids)

    def __contains__(self, ref: str) -> bool:
        return self.has_ref(ref)

    def __eq__(self, other: "RefSet") -> bool:
        return self._ids == other._ids

    def __iter__(self) -> Iterator:
        """Returns a series of full references (e.g. "DocumentReference/1")"""
        for res_type, ids in self._ids.items():
            for ref_id in ids:
                yield f"{res_type}/{ref_id}"

    def __len__(self) -> int:
        return sum(len(ids) for ids in self._ids.values())

    def __str__(self) -> str:
        return "{" + ", ".join(sorted(self)) + "}"


#####################
# Table ref discovery
#####################


class RefsNotFound(ValueError):
    pass


RefScanner = Callable[[Sequence[str]], str | None]


def make_note_ref_scanner(columns: Iterable[str], *, is_anon: bool = False) -> RefScanner:
    """
    Returns a scanner function you can call on rows of data, to find note and patient references.

    This is useful if you have a csv file or a SQL table that you want to search through for
    columns that indicate note resource refs.

    It will look for columns like note_ref, documentreference_id, etc.

    Patient refs are only included as a last resort, if note IDs columns are not found.
    """
    columns = [col.casefold() for col in columns]
    return _make_ref_getter(columns, is_anon=is_anon)


def _make_ref_getter(fieldnames: Sequence[str], *, is_anon: bool = False) -> RefScanner:
    """Returns a callable that returns (note ref, patient ref)"""
    get_dxr = _find_header(fieldnames, "DiagnosticReport", is_anon=is_anon)
    get_doc = _find_header(fieldnames, "DocumentReference", is_anon=is_anon)
    get_pat = _find_header(fieldnames, "Patient", is_anon=is_anon)

    if not get_dxr and not get_doc and not get_pat:
        raise RefsNotFound("No patient or note IDs found.")

    # A method that takes a row of a table and returns a patient/note ref from it
    def getter(row: Sequence[str]) -> str | None:
        if get_dxr:
            if val := get_dxr(row):
                return val
        if get_doc:
            if val := get_doc(row):
                return val
        # If and only if we don't have any resource ID matchers, we'll check by patient
        if not get_dxr and not get_doc and get_pat:
            if val := get_pat(row):
                return val
        return None

    return getter


def _find_header(
    fieldnames: Sequence[str], res_type: str, *, is_anon: bool = False
) -> Callable[[Sequence[str]], str | None] | None:
    folded = res_type.casefold()
    id_names = [f"{folded}_id"]
    ref_names = [f"{folded}_ref"]

    if res_type in {"DiagnosticReport", "DocumentReference"}:
        ref_names.append("document_ref")
        ref_names.append("note_ref")
    if res_type == "DocumentReference":
        id_names.append("docref_id")
    if res_type == "Patient":
        id_names.append("subject_id")
        ref_names.append("subject_ref")

    if is_anon:
        # Look for both anon_ and normal versions, but prefer an explicit column in case both exist
        id_names = [f"anon_{x}" for x in id_names] + id_names
        ref_names = [f"anon_{x}" for x in ref_names] + ref_names

    for field in id_names:
        if field in fieldnames:
            idx = fieldnames.index(field)
            return lambda x: f"{res_type}/{x[idx]}" if x[idx] else None
    for field in ref_names:
        if field in fieldnames:
            idx = fieldnames.index(field)
            prefix = f"{res_type}/"
            return lambda x: x[idx] if x[idx].startswith(prefix) else None

    return None


################
# Note filtering
################


class NoteFilter(Protocol):
    def __call__(self, note_res: dict, *, text: str | None = None) -> bool:
        """
        Takes a note resource (DxReport or DocRef) and returns True/False.

        If `text` is None, the text will be pulled from the note resource (so you only need to pass
        it if you already have it or as an optimization over multiple NoteFilter calls).
        """


_ESCAPED_WHITESPACE = re.compile(r"(\\\s)+")


def make_note_filter(
    *,
    reject_by_regex: Iterable[str] | None = None,
    reject_by_word: Iterable[str] | None = None,
    select_by_regex: Iterable[str] | None = None,
    select_by_word: Iterable[str] | None = None,
    select_by_ref: RefSet | None = None,
    salt: bytes | None = None,
) -> NoteFilter:
    """
    Creates a callable NoteFilter that encodes the select_by and reject_by arguments.

    Even if passed no arguments, note status is always checked (to reject preliminary or
    entered-in-error notes).

    The select_by_ref argument will be matched on direct DocumentReference or DiagnosticReport
    matches, as well as Patient matches for note subjects.

    Provide a salt if select_by_ref holds anonymized refs.
    """

    pattern = _compile_filter_regex(
        reject_by_regex=reject_by_regex,
        reject_by_word=reject_by_word,
        select_by_regex=select_by_regex,
        select_by_word=select_by_word,
    )

    def note_filter(note_res: dict, *, text: str | None = None) -> bool:
        if not _filter_status(note_res):
            return False

        if not _filter_refs(note_res, salt=salt, select_by_ref=select_by_ref):
            return False

        if pattern is not None:
            if text is None:
                try:
                    text = get_text_from_note_res(note_res)
                except Exception:
                    text = ""  # to allow passing if we only have reject_by_*

            if pattern.search(text) is None:
                return False

        return True

    return note_filter


def _filter_status(note_res: dict) -> bool:
    """If the resource status is WIP, obsolete, or entered-in-error, reject it"""
    note_type = note_res.get("resourceType")
    note_id = note_res.get("id")

    # Require basic resource fields, so that other filters can use these without guards
    if not note_type or not note_id:
        return False

    match note_type:
        case "DiagnosticReport":
            valid_status_types = {"final", "amended", "corrected", "appended", "unknown", None}
            return note_res.get("status") in valid_status_types

        case "DocumentReference":
            good_status = note_res.get("status") in {"current", None}  # status of DocRef itself
            # docStatus is status of clinical note attachments
            good_doc_status = note_res.get("docStatus") in {"final", "amended", None}
            return good_status and good_doc_status

        case _:
            return False


def _filter_refs(
    note_res: dict,
    *,
    salt: bytes | None,
    select_by_ref: RefSet | None,
) -> bool:
    """Returns False if refs are provided and this note doesn't match any of them"""
    if not select_by_ref:
        return True

    if select_by_ref.has_type("Patient"):
        # Both DxReports and DocRefs use subject
        subject_ref = anon_ref(note_res.get("subject", {}).get("reference"), salt)
        if subject_ref not in select_by_ref:
            return False

    if select_by_ref.has_type("DiagnosticReport") or select_by_ref.has_type("DocumentReference"):
        note_ref = f"{note_res['resourceType']}/{anon_id(note_res['id'], salt)}"
        if note_ref not in select_by_ref:
            return False

    return True


def _user_regex_to_pattern(term: str) -> str:
    """Takes a user search regex and adds some boundaries to it"""
    # Make a custom version of \b that allows non-word characters to be on edge of the term too.
    # For example:
    #   This misses: re.match(r"\ba\+\b", "a+")
    #   But this hits: re.match(r"\ba\+", "a+")
    # So to work around that, we look for the word boundary ourselves.
    edge = r"(\W|$|^)"
    return f"{edge}({term}){edge}"


def _user_word_to_pattern(term: str) -> str:
    """Takes a user search term and turns it into a clinical-note-appropriate regex"""
    term = re.escape(term)
    # Allow multi-word "words" (like "severe cough") have any kind of whitespace in between them,
    # as they may cross line endings in the note (which can happen for normal paragraph wrapping).
    term = _ESCAPED_WHITESPACE.sub(r"\\s+", term)
    return _user_regex_to_pattern(term)


def _combine_regexes(*, by_regex: Iterable[str] | None, by_word: Iterable[str] | None) -> str:
    patterns = []
    if by_regex:
        patterns.extend(_user_regex_to_pattern(regex) for regex in set(by_regex))
    if by_word:
        patterns.extend(_user_word_to_pattern(word) for word in set(by_word))
    return "|".join(patterns)


def _compile_filter_regex(
    *,
    reject_by_regex: Iterable[str] | None,
    reject_by_word: Iterable[str] | None,
    select_by_regex: Iterable[str] | None,
    select_by_word: Iterable[str] | None,
) -> re.Pattern | None:
    select = _combine_regexes(by_word=select_by_word, by_regex=select_by_regex)

    reject = _combine_regexes(by_word=reject_by_word, by_regex=reject_by_regex)
    if reject:
        # Use negative lookahead
        reject = rf"^(?!.*{reject})"

    if reject and select:
        # Add positive lookahead
        final = f"{reject}(?=.*{select})"
    elif reject:
        final = reject
    elif select:
        final = select
    else:
        return None

    return re.compile(final, re.IGNORECASE)
