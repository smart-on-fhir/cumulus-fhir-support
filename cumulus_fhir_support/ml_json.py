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

This module supports file access via fsspec (either directly or via FsPath).

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

import json
import logging
import pathlib
import re
from collections.abc import Generator, Iterable
from typing import Any

import fsspec

from . import io, resource_info

PathType = str | pathlib.Path | io.FsPath
ResourceType = str | Iterable[str] | None

logger = logging.getLogger(__name__)

NONLETTER = re.compile(r"[^a-zA-Z]+")


def list_multiline_json_in_dir(
    path: PathType,
    resource: ResourceType = None,
    *,
    fsspec_fs: fsspec.AbstractFileSystem | None = None,
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
    - Resource type matching will first examine the filename and if unsure, read the first line.

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
    if not isinstance(path, io.FsPath):
        path = io.FsPath(path, fs=fsspec_fs)

    children = path.ls(include_dirs=False, recursive=recursive)

    # Coalesce resource to None or a set of strings
    if isinstance(resource, str):
        resource = {resource}
    elif resource is not None:
        resource = set(resource)

    # Now grab filenames for all target resource types
    results = {}
    for child in sorted(children):  # sorted as an API promise
        results.update(_get_resource_type(child, resource))
    return results


def _get_resource_type(
    path: io.FsPath,
    target_resources: set[str] | None,
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
    :return: a tiny dict of {path: resourceType} if the file is valid else {}
    """
    # Must look like a multi-line JSON file
    good_endings = {".jsonl", ".ndjson"}
    good_compressions = {".gz"}
    suffixes = [x.casefold() for x in path.suffixes]
    valid_filename = (len(suffixes) > 0 and suffixes[-1] in good_endings) or (
        len(suffixes) > 1 and suffixes[-2] in good_endings and suffixes[-1] in good_compressions
    )
    if not valid_filename:
        return {}

    # Apply a heuristic to try to avoid actually reading the file, which can be slow if we are
    # scanning over a network:
    # - If a single resource type appears directly in a filename fragment, assume it applies.
    # We avoid counting a PractitionerRole.ndjson file as a Practitioner file by only looking for
    # complete words.
    pieces = set(NONLETTER.split(path.name))
    found_types = pieces & resource_info.ALL_RESOURCES
    if len(found_types) == 1:
        resource_type = found_types.pop()
    else:
        # Try reading from file to see what resource type it holds.
        try:
            # Check just the first record, as all records in a file should be the same resource.
            # See https://www.hl7.org/fhir/R4/nd-json.html
            #
            # And since we cannot assume that "resourceType" is the first field,
            # we must parse the whole first line.
            # See https://www.hl7.org/fhir/R4/json.html#resources
            if not (line := _read_first_line(path)):
                return {}
            parsed = json.loads(line)
        except Exception as exc:
            logger.warning("Could not read from '%s': %s", str(path), str(exc))
            return {}

        resource_type = parsed.get("resourceType") if isinstance(parsed, dict) else None

    if target_resources is None or resource_type in target_resources:
        return {str(path): resource_type}

    # Didn't match our target resource types, just pretend it doesn't exist
    return {}


def _read_first_line(path: io.FsPath) -> bytes:
    # We just want the first line, and nothing else. The fsspec s3 block size default is 50M,
    # larger than we usually need for FHIR files. So we try to speed things up by just sipping
    # what we need. 9k is usually enough to only need one read call for gzipped files, at least
    # for all but the beefier inlined DocRefs.
    with path.open("rb", block_size=9000) as f:
        return f.readline().rstrip(b"\r\n")


def read_multiline_json_with_details(
    path: PathType,
    *,
    fsspec_fs: fsspec.AbstractFileSystem | None = None,
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
    if not isinstance(path, io.FsPath):
        path = io.FsPath(path, fs=fsspec_fs)

    try:
        with path.open("rb") as f:
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
                    logger.warning(
                        "Could not decode '%s:%d': %s", str(path), line_num + 1, str(exc)
                    )
    except Exception as exc:
        logger.error("Could not read from '%s': %s", str(path), str(exc))


def read_multiline_json(
    path: PathType,
    *,
    fsspec_fs: fsspec.AbstractFileSystem | None = None,
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
    fsspec_fs: fsspec.AbstractFileSystem | None = None,
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
