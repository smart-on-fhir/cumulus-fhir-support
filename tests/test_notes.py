import base64
import binascii
import unittest

import ddt

import cumulus_fhir_support as cfs

# Some convenience salt values to use
SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


@ddt.ddt
class NoteTests(unittest.TestCase):
    @ddt.data(
        (None, None),
        ("", None),
        ("abc", "75b245a08d21040487dde2efe7038f93ea7ecb06dfc1dc7275e4ad3a22f57a22"),
    )
    @ddt.unpack
    def test_anon_id(self, id_val, expected):
        assert cfs.anon_id(id_val, SALT_BYTES) == expected

    @ddt.data(
        (None, None),
        ("", None),
        ("abc", None),
        ("Device/abc", "Device/75b245a08d21040487dde2efe7038f93ea7ecb06dfc1dc7275e4ad3a22f57a22"),
    )
    @ddt.unpack
    def test_anon_ref(self, ref_val, expected):
        assert cfs.anon_ref(ref_val, SALT_BYTES) == expected

    @ddt.data(
        (  # simple DxReport text
            "DiagnosticReport",
            [("text/plain", "hello", "url")],
            "hello",
        ),
        (  # simple DocRef text
            "DocumentReference",
            [("text/plain", "hello", "url")],
            "hello",
        ),
        (  # prefer text over any html variant
            "DocumentReference",
            [
                ("text/html", "html", None),
                ("text/plain", "text", None),
                ("application/xhtml+xml", "xhtml", None),
            ],
            "text",
        ),
        (  # prefer html over xhtml
            "DocumentReference",
            [("text/html", "html", None), ("application/xhtml+xml", "xhtml", None)],
            "html",
        ),
        (  # but accept xhtml
            "DocumentReference",
            [("application/xhtml+xml", "xhtml", None)],
            "xhtml",
        ),
        (  # strips html
            "DocumentReference",
            [("text/html", "<html><body>He<b>llooooo</b></html>", None)],
            "Hellooooo",
        ),
        (  # strips xhtml
            "DocumentReference",
            [("application/xhtml+xml", "<html><body>He<b>llooooo</b></html>", None)],
            "Hellooooo",
        ),
        (  # does not strips text
            "DocumentReference",
            [("text/plain", "<html><body>He<b>llooooo</b></html>", None)],
            "<html><body>He<b>llooooo</b></html>",
        ),
        (  # strips surrounding whitespace
            "DocumentReference",
            [("text/plain", "\n\n hello   world \n\n", None)],
            "hello   world",
        ),
        (  # respects charset
            "DiagnosticReport",
            [("text/plain; charset=utf16", b"\xff\xfeh\x00e\x00l\x00l\x00o\x00", None)],
            "hello",
        ),
        (  # bad charset
            "DiagnosticReport",
            [("text/plain", b"\xff\xfeh\x00e\x00l\x00l\x00o\x00", None)],
            (UnicodeDecodeError, "invalid start byte"),
        ),
        (  # unsupported mime type
            "DiagnosticReport",
            [("application/pdf", "pdf", None)],
            (ValueError, "No textual mimetype found"),
        ),
        (  # no attachments
            "DiagnosticReport",
            [],
            (ValueError, "No textual mimetype found"),
        ),
        (  # url only
            "DiagnosticReport",
            [("text/plain", None, "url")],
            (cfs.RemoteAttachment, "only available via URL"),
        ),
        (  # bad resource type
            "Patient",
            [],
            (ValueError, "Patient is not a supported clinical note type"),
        ),
        (  # no data or url
            "DocumentReference",
            [("text/plain", None, None)],
            (ValueError, "No data or url field present"),
        ),
    )
    @ddt.unpack
    def test_get_text_from_note_res(self, res_type, attachments, result):
        note_res = {"resourceType": res_type}

        # Build attachment list
        attachments = [
            {
                "contentType": attachment[0],
                "data": attachment[1],
                "url": attachment[2],
            }
            for attachment in attachments
        ]
        for attachment in attachments:
            if data := attachment["data"]:
                if isinstance(data, str):
                    data = data.encode()
                attachment["data"] = base64.standard_b64encode(data).decode()
        if res_type == "DiagnosticReport":
            note_res["presentedForm"] = attachments
        elif res_type == "DocumentReference":
            note_res["content"] = [{"attachment": a} for a in attachments]

        # Grab text and compare
        if isinstance(result, str):
            assert cfs.note_res_has_text(note_res) is True
            assert cfs.get_text_from_note_res(note_res) == result
        else:
            has_text = result[0] is UnicodeDecodeError
            assert cfs.note_res_has_text(note_res) == has_text, result
            with self.assertRaisesRegex(*result):
                cfs.get_text_from_note_res(note_res)

    def test_ref_set(self):
        assert bool(cfs.RefSet()) is False

        # Inspecting the set
        refs = cfs.RefSet("Device/d", "Patient/p")
        assert bool(refs) is True
        assert list(refs) == ["Device/d", "Patient/p"]
        assert str(refs) == "{Device/d, Patient/p}"
        assert refs == cfs.RefSet("Device/d", "Patient/p")
        assert refs != cfs.RefSet("Patient/p")
        assert refs.has_type("Patient")
        assert not refs.has_type("Condition")
        assert refs.has_ref("Device/d")
        assert not refs.has_ref("Condition/c")
        assert not refs.has_ref(None)
        assert not refs.has_ref("other text")
        assert "Patient/p" in refs
        assert "Patient/x" not in refs
        assert refs.has_id("Device", "d")
        assert not refs.has_id("Condition", "c")

        # Modifying the set
        new_refs = cfs.RefSet(refs)
        assert refs == new_refs
        refs.add_id("Encounter", "e")
        refs.add_ref("Observation/o")
        assert refs != new_refs
        new_refs.add_set(refs)
        assert refs == new_refs
        assert len(refs) == 4

        # Attached data
        data = ["extra", "data"]
        refs = cfs.RefSet()
        refs.add_id("Device", "d", data=data)
        refs.add_ref("Patient/p", data=5)
        assert refs.get_data_for_id("Patient", "x", default=10) == 10
        assert refs.get_data_for_id("Patient", "p", default=10) == 5
        assert refs.get_data_for_ref("Observation/x") is None
        assert refs.get_data_for_ref("Observation/x", default="hello") == "hello"
        assert refs.get_data_for_ref("Device/d") is data
        new_refs = cfs.RefSet(refs)  # will deepcopy data
        assert new_refs.get_data_for_id("Patient", "p") == 5
        assert new_refs.get_data_for_ref("Device/d") == data
        assert new_refs.get_data_for_ref("Device/d") is not data

    @ddt.data(
        (  # basic id match
            "patient_id,DocumentREFERENCE_ID",
            "xxx,yyy",
            "DocumentReference/yyy",
        ),
        (  # basic ref match
            "patient_id,diagnosticreport_ref",
            "xxx,DiagnosticReport/yyy",
            "DiagnosticReport/yyy",
        ),
        (  # custom document_ref alias
            "patient_id,document_ref",
            "xxx,DiagnosticReport/ref",
            "DiagnosticReport/ref",
        ),
        (  # custom note_ref alias
            "patient_id,note_ref",
            "xxx,DocumentReference/ref",
            "DocumentReference/ref",
        ),
        (  # custom docref_id alias
            "patient_id,docref_id",
            "xxx,ref",
            "DocumentReference/ref",
        ),
        (  # patient id match (can't be note ref in there)
            "patient_id,other_col",
            "xxx,blarg",
            "Patient/xxx",
        ),
        (  # patient ref match (can't be note ref in there)
            "patient_ref,other_col",
            "Patient/abc,blarg",
            "Patient/abc",
        ),
        (  # custom subject_id alias
            "subject_id,other_col",
            "abc,blarg",
            "Patient/abc",
        ),
        (  # custom subject_ref alias
            "subject_ref,other_col",
            "Patient/abc,blarg",
            "Patient/abc",
        ),
        (  # prefer anon version of id columns
            "anon_patient_id,patient_id",
            "anon,orig",
            "Patient/anon",
        ),
        (  # prefer anon version of ref columns
            "note_ref,anon_note_ref",
            "DocumentReference/orig,DocumentReference/anon",
            "DocumentReference/anon",
        ),
        (  # with competing columns, we will pick the non-empty one
            "diagnosticreport_id,documentreference_id",
            ",docref",
            "DocumentReference/docref",
        ),
        (  # unsupported ref type
            "note_ref",
            "Patient/abc",
            None,
        ),
        (  # no valid cols found
            "other,nope",
            "xxx,yyy",
            ValueError,
        ),
    )
    @ddt.unpack
    def test_make_note_ref_scanner(self, cols, row, result):
        cols = cols.split(",")
        row = row.split(",")

        if isinstance(result, type):
            with self.assertRaises(result):
                cfs.make_note_ref_scanner(cols)
        else:
            scanner = cfs.make_note_ref_scanner(cols, is_anon=True)
            assert result == scanner(row), cols

    @ddt.data(
        (  # default, no regexes, no status - should pass
            {"resourceType": "DiagnosticReport", "id": "1"},
            "",
            {},
            True,
        ),
        (  # unsupported type
            {"resourceType": "Patient", "id": "1"},
            "",
            {},
            False,
        ),
        (  # No ID
            {"resourceType": "DiagnosticReport"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "id": "1", "status": "registered"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "id": "1", "status": "partial"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "id": "1", "status": "preliminary"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "id": "1", "status": "cancelled"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "id": "1", "status": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DocumentReference", "id": "1", "status": "superseded"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DocumentReference", "id": "1", "status": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # bad docStatus
            {"resourceType": "DocumentReference", "id": "1", "docStatus": "preliminary"},
            "",
            {},
            False,
        ),
        (  # bad docStatus
            {"resourceType": "DocumentReference", "id": "1", "docStatus": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # select word (negative case)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_word": {"bye"}},
            False,
        ),
        (  # select word (positive case)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello, world",
            {"select_by_word": {"hello"}},
            True,
        ),
        (  # select word (multiple selections, or'd together)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_word": {"bye", "hello"}},
            True,
        ),
        (  # select word (weird characters)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hel*lo.1+ world",
            {"select_by_word": {"hel*lo.1+"}},
            True,
        ),
        (  # select word (substring isn't matched)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_word": {"hell"}},
            False,
        ),
        (  # select word (multi word)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world, mr smith",
            {"select_by_word": {"mr smith"}},
            True,
        ),
        (  # select regex (matches)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_regex": {"hell."}},
            True,
        ),
        (  # select regex (can cross word boundaries)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_regex": {"hell.*d"}},
            True,
        ),
        (  # select regex (can cross word boundaries, but still respects word ends)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_regex": {"hell.*r"}},
            False,
        ),
        (  # select word and regex (matches either)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"select_by_word": {"world"}, "select_by_regex": {"h."}},
            True,
        ),
        (  # reject word (by itself, without matching, we should select note)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_word": {"bye"}},
            True,
        ),
        (  # reject word (by itself, with matching, we should reject note)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_word": {"hello"}},
            False,
        ),
        (  # reject word (multiple options, will reject either)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_word": {"hello", "bye"}},
            False,
        ),
        (  # reject word (and select it, reject should win)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_word": {"hello"}, "select_by_word": {"hello"}},
            False,
        ),
        (  # reject word (miss) and select word (hit)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_word": {"bye"}, "select_by_word": {"hello"}},
            True,
        ),
        (  # reject regex (simple match)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_regex": {"he..o"}},
            False,
        ),
        (  # reject regex (across words)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_regex": {"he.*rld"}},
            False,
        ),
        (  # reject word and regex (either should reject)
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello world",
            {"reject_by_regex": {"he..o"}, "reject_by_word": {"bye"}},
            False,
        ),
        (  # select on non-first lines
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello\nworld",
            {"select_by_word": {"world"}},
            True,
        ),
        (  # reject on non-first lines
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello\nworld",
            {"reject_by_word": {"world"}},
            False,
        ),
        (  # select across lines and whitespace, if multiple words provided
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello  \n  world",
            {"select_by_word": {"hello world"}},
            True,
        ),
        (  # (don't) select across lines with other stuff in there, confirming a lack of match
            {"resourceType": "DiagnosticReport", "id": "1"},
            "hello\n.world",
            {"select_by_word": {"hello world"}},
            False,
        ),
        (  # select note ref
            {"resourceType": "DocumentReference", "id": "1"},
            "",
            {
                "select_by_ref": cfs.RefSet(
                    "DocumentReference/69123f5b2305aba4bc734b41c66cedab639b3e81d4ae8eeb9569d6dc1476a1e7"
                )
            },
            True,
        ),
        (  # select note ref and word (miss on ref, so we never consider the word)
            {"resourceType": "DocumentReference", "id": "1"},
            "hello world",
            {"select_by_ref": cfs.RefSet("DocumentReference/xxx"), "select_by_word": {"world"}},
            False,
        ),
        (  # select patient ref
            {"resourceType": "DiagnosticReport", "id": "1", "subject": {"reference": "Patient/1"}},
            "",
            {
                "select_by_ref": cfs.RefSet(
                    "Patient/69123f5b2305aba4bc734b41c66cedab639b3e81d4ae8eeb9569d6dc1476a1e7"
                )
            },
            True,
        ),
        (  # select patient ref (miss)
            {"resourceType": "DiagnosticReport", "id": "1", "subject": {"reference": "Patient/1"}},
            "",
            {"select_by_ref": cfs.RefSet("Patient/xxx")},
            False,
        ),
        (  # pulls text if not provided ("hello world")
            {
                "resourceType": "DiagnosticReport",
                "id": "1",
                "presentedForm": [{"contentType": "text/plain", "data": "aGVsbG8gd29ybGQ="}],
            },
            None,
            {"select_by_word": {"hello"}},
            True,
        ),
        (  # select by word with no text at all (fails)
            {"resourceType": "DiagnosticReport", "id": "1"},
            None,
            {"select_by_word": {"hello"}},
            False,
        ),
        (  # reject by word with no text at all (passes)
            {"resourceType": "DiagnosticReport", "id": "1"},
            None,
            {"reject_by_word": {"hello"}},
            True,
        ),
    )
    @ddt.unpack
    def test_note_filter(self, res, text, kwargs, selected):
        note_filter = cfs.make_note_filter(**kwargs, salt=SALT_BYTES)
        assert note_filter(res, text=text) is selected, kwargs

    def test_note_filter_no_salt(self):
        doc = {"resourceType": "DiagnosticReport", "id": "1"}

        note_filter = cfs.make_note_filter(select_by_ref=cfs.RefSet("DiagnosticReport/0"))
        assert note_filter(doc) is False

        note_filter = cfs.make_note_filter(select_by_ref=cfs.RefSet("DiagnosticReport/1"))
        assert note_filter(doc) is True
