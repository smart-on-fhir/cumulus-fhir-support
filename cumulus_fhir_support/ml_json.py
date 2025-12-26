"""
Find and read multi-line JSON files.

FHIR multi-line JSON files can come in many different filename pattern flavors.
And file parsing can have its own oddities.
These utility functions allow the Cumulus family of tools to all handle
being given "a folder of NDJSON input" the same way.

** Error handling

In general, these functions log and ignore errors.
This library is optimized for parsing large amounts of externally-provided JSON,
where aborting on a single error you may not have control over rarely makes sense.

** fsspec

This module has optional support for file access via fsspec.
It is not a required dependency, but will be used if provided.

** File format

There are two multi-line JSON specs at the time of writing:
- https://github.com/ndjson/ndjson-spec (unmaintained but more popular at time of writing)
  - See https://github.com/ndjson/ndjson-spec/issues/35 for more details
  - Notably, ndjson.org was allowed to expire and is now a gambling site
- https://jsonlines.org/ (maintained)

The only real differences are:
- different file extensions (.ndjson vs .jsonl)
- NDJSON allows parsers to ignore empty lines

This module splits the difference by looking for both extensions and allowing empty lines.
Which isn't that different from parsing empty lines and printing an error about it.

Though in general, the FHIR world seems to prefer NDJSON for multi-line JSON,
including referencing it by name in the spec and some mimetypes
(see https://www.hl7.org/fhir/nd-json.html).
"""

import gzip
import json
import logging
import os
import pathlib
from collections.abc import Generator, Iterable
from typing import TYPE_CHECKING, Any, BinaryIO, Optional

if TYPE_CHECKING:
    import fsspec  # pragma: no cover

PathType = str | pathlib.Path
ResourceType = str | Iterable[str] | None

logger = logging.getLogger(__name__)


def list_multiline_json_in_dir(
    path: PathType,
    resource: ResourceType = None,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
    recursive: bool = False,
) -> dict[str, str | None]:
    """
    Returns file info in the target folder that are multi-line JSON files for the given resources.

    - I/O and JSON errors will be logged, not raised.
    - Will return an empty dict if the path does not exist.
    - Passing None as the resource filter (the default) will return all multi-line JSON found.
    - Returned filenames will be full paths.
    - The order of returned filenames will be consistent across calls (Python sort order).
    - This function will notice both JSON Lines (.jsonl) and NDJSON (.ndjson) files.
    - Symlinks will be followed and the target destination will be returned.

    Examples:
    list_multiline_json_in_dir("/") -> {
        "/random.jsonl": None,
        "/con1.ndjson": "Condition",
        "/pat1.jsonl": "Patient",
    }
    list_multiline_json_in_dir("/", "Patient") -> {"/pat1.jsonl": "Patient"}
    list_multiline_json_in_dir("/", ["Condition", "Patient"]) -> {
        "/con1.ndjson": "Condition",
        "/pat1.jsonl": "Patient",
    }

    :param path: the folder to examine
    :param resource: the type of FHIR resource(s) for which to return files
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :param recursive: whether to recursively search subfolders
    :return: a dict of {path: resourceType} for all child files of the appropriate type(s)
    """
    if fsspec_fs:
        children = _list_fsspec_files(fsspec_fs, str(path), recursive=recursive)
    else:
        children = _list_local_files(pathlib.Path(path), recursive=recursive)

    # Coalesce resource to None or a set of strings
    if isinstance(resource, str):
        resource = {resource}
    elif resource is not None:
        resource = set(resource)

    # Now grab filenames for all target resource types
    results = {}
    for child in sorted(children):  # sorted as an API promise
        results.update(_get_resource_type(child, resource, fsspec_fs=fsspec_fs))
    return results


def _list_fsspec_files(
    fsspec_fs: "fsspec.AbstractFileSystem",
    path: str,
    *,
    recursive: bool = False,
    visited: set[str] | None = None,
) -> set[str]:
    if not fsspec_fs.exists(path):
        return set()
    visited = visited or set()
    results = set()

    if recursive:
        items = fsspec_fs.find(path, detail=True).values()
    elif visited:
        items = [fsspec_fs.info(path)]  # no iteration as we follow links
    else:
        items = fsspec_fs.ls(path, detail=True)

    for details in items:
        full = details["name"]
        if details.get("islink") and details.get("destination"):
            resolved = os.path.join(os.path.dirname(full), details.get("destination"))
            resolved = os.path.normpath(resolved)
            was_visited = resolved in visited
            visited.add(resolved)
            if not was_visited:
                results |= _list_fsspec_files(
                    fsspec_fs, resolved, recursive=recursive, visited=visited
                )
        elif details.get("type") == "file":
            results.add(full)
    return results


def _list_local_files(path: pathlib.Path, recursive: bool = False) -> set[str]:
    if not path.exists():
        return set()
    results = set()
    for dirpath, _dirnames, filenames in os.walk(path, followlinks=True):
        if dirpath != str(path) and not recursive:
            continue
        for filename in filenames:
            full = pathlib.Path(dirpath) / filename
            resolved = full.resolve()
            if resolved.is_file():
                results.add(str(resolved))
    return results


def _open(
    path: PathType,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
    fsspec_kwargs: dict | None = None,
) -> BinaryIO:
    """Opens a file with optional compression and fsspec"""
    if fsspec_fs:
        fsspec_kwargs = fsspec_kwargs or {}
        return fsspec_fs.open(str(path), compression="infer", **fsspec_kwargs)

    suffix = pathlib.Path(path).suffix.casefold()
    if suffix == ".gz":
        return gzip.open(path)
    else:
        return open(path, "rb")


def _get_resource_type(
    path: str,
    target_resources: set[str] | None,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> dict[str, str | None]:
    """
    Returns path & resource type if the file appears to be for the given resources.

    ** Digression into the wide world of FHIR multi-line filenames **

    Here's the filenames that the "official" tools use:
    - bulk-data-client creates files like "1.Condition.ndjson"
    - cumulus-etl creates files like "Condition.001.ndjson"

    The servers themselves aren't any help / give no guidance, if you were doing raw bulk:
    - Cerner provides download files like "11ef-34be-b3c0dc02-87c5-6a5c9bab18ec"
    - Epic provides download files like "eNZwsy9bU7LX8nBB.RJXkpA3"

    Plus sometimes you don't have full control of the filenames.
    We've seen IT groups provide bulk files like "Condition20240614-112115.ndjson"
    on a shared drive that you don't control.

    But even if you do control the filename, it might be convenient to rename the files
    with some extra context like "vital-signs.ndjson" or whatever the case may be.

    Because of that wide variety, we'll avoid assuming anything about the filename.
    Instead, we'll just sniff the first line of every file to examine whether it matches the target
    resource.

    We will look for an expected file extension at least, which the bulk servers don't provide,
    but it seems reasonable to require to avoid reading the first line of big binary files.

    :param path: the file to examine
    :param target_resources: the type of FHIR resource(s) to accept for this file
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :return: a tiny dict of {path: resourceType} if the file is valid else {}
    """
    # Must look like a multi-line JSON file
    good_endings = {".jsonl", ".ndjson"}
    good_compressions = {".gz"}
    suffixes = [x.casefold() for x in pathlib.Path(path).suffixes]
    valid_filename = (len(suffixes) > 0 and suffixes[-1] in good_endings) or (
        len(suffixes) > 1 and suffixes[-2] in good_endings and suffixes[-1] in good_compressions
    )
    if not valid_filename:
        return {}

    try:
        # Check just the first record, as all records in a file should be the same resource.
        # See https://www.hl7.org/fhir/R4/nd-json.html
        #
        # And since we cannot assume that "resourceType" is the first field,
        # we must parse the whole first line.
        # See https://www.hl7.org/fhir/R4/json.html#resources
        if not (line := _read_first_line(path, fsspec_fs=fsspec_fs)):
            return {}
        parsed = json.loads(line)
    except Exception as exc:
        logger.warning("Could not read from '%s': %s", path, str(exc))
        return {}

    resource_type = parsed.get("resourceType") if isinstance(parsed, dict) else None

    if target_resources is None or resource_type in target_resources:
        return {path: resource_type}

    # Didn't match our target resource types, just pretend it doesn't exist
    return {}


def _read_first_line(
    path: PathType,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> bytes:
    # We just want the first line, and nothing else. The fsspec s3 block size default is 50M,
    # larger than we usually need for FHIR files. So we try to speed things up by just sipping
    # what we need. 9k is usually enough to only need one read call for gzipped files, at least
    # for all but the beefier inlined DocRefs.
    with _open(path, fsspec_fs=fsspec_fs, fsspec_kwargs={"block_size": 9000}) as f:
        return f.readline().rstrip(b"\r\n")


def read_multiline_json_with_details(
    path: PathType,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
    offset: int = 0,
) -> Generator[dict[str, Any]]:
    """
    Generator that yields lines of JSON from the target file, plus extra metadata.

    Returned fields:
    - "json": the parsed line of content
    - "line_num": the line number (counting from `offset`)
    - "byte_offset": the byte offset (counting from `offset`)

    - I/O and JSON errors will be logged, not raised.
    - Will return an empty result if the path does not exist or is not readable.
    - Empty lines in the source file will be skipped (but will be represented in the offsets).
    - The lines of JSON are not required to be dictionaries.
    - Returned line-numbers/byte-offsets and are zero-based _from_ the provided offset

    :param path: the file to read
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :param offset: optionally, how far to seek into the file before returning results
    :return: a generator of dictionaries, with per-line info
    """
    try:
        with _open(path, fsspec_fs=fsspec_fs) as f:
            if offset:
                f.seek(offset)
            byte_total = 0
            for line_num, line in enumerate(f):
                byte_num = byte_total
                byte_total += len(line)
                if not line.rstrip(b"\r\n"):
                    # ignore empty lines (shouldn't normally happen,
                    # but maybe the file has an extra trailing new line
                    # or some other oddity - let's be graceful)
                    continue
                try:
                    yield {"json": json.loads(line), "line_num": line_num, "byte_offset": byte_num}
                except json.JSONDecodeError as exc:
                    logger.warning("Could not decode '%s:%d': %s", path, line_num + 1, str(exc))
    except Exception as exc:
        logger.error("Could not read from '%s': %s", path, str(exc))


def read_multiline_json(
    path: PathType,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> Generator[Any]:
    """
    Generator that yields lines of JSON from the target file.

    - I/O and JSON errors will be logged, not raised.
    - Will return an empty result if the path does not exist or is not readable.
    - Empty lines in the source file will be skipped.
    - The lines of JSON are not required to be dictionaries.

    :param path: the file to read
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :return: a generator of parsed JSON results, line by line
    """
    for line in read_multiline_json_with_details(path, fsspec_fs=fsspec_fs):
        yield line["json"]


def read_multiline_json_from_dir(
    path: PathType,
    resource: ResourceType = None,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
    recursive: bool = False,
) -> Generator[Any]:
    """
    Generator that yields lines of JSON from the target folder.

    - I/O and JSON errors will be logged, not raised.
    - Will return an empty result if the path does not exist or is not readable.
    - Passing None as the resource filter (the default) will return all multi-line JSON found.
    - The lines of JSON are not required to be dictionaries.
    - The order of results will be consistent across calls (filenames are Python-sorted first,
      then rows are returned from each file in order, top to bottom)
    - This function will notice both JSON Lines (.jsonl) and NDJSON (.ndjson) files.
    - Symlinks will be followed.

    :param path: the folder to scan
    :param resource: the type of FHIR resource(s) for which to return files
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :param recursive: whether to recursively search subfolders
    :return: a generator of parsed JSON results, line by line
    """
    for filename in list_multiline_json_in_dir(
        path, resource, fsspec_fs=fsspec_fs, recursive=recursive
    ):
        yield from read_multiline_json(filename, fsspec_fs=fsspec_fs)
