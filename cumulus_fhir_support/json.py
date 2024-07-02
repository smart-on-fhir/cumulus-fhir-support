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

import json
import logging
import os
import pathlib
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union

if TYPE_CHECKING:
    import fsspec

PathType = Union[str, pathlib.Path]
ResourceType = Union[str, Iterable[str], None]

logger = logging.getLogger(__name__)


def list_multiline_json_in_dir(
    path: PathType,
    resource: ResourceType = None,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> dict[str, Optional[str]]:
    """
    Returns file info in the target folder that are multi-line JSON files for the given resources.

    - This will not recurse into sub-folders.
    - I/O and JSON errors will be logged, not raised.
    - Will return an empty dict if the path does not exist.
    - Passing None as the resource filter (the default) will return all multi-line JSON found.
    - Returned filenames will be full paths.
    - The order of filenames will be consistent across calls.
    - This function will notice both JSON Lines (.jsonl) and NDJSON (.ndjson) files.

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
    :return: a dict of {path: resourceType} for all child files of the appropriate type(s)
    """
    path = str(path)
    if fsspec_fs:
        if not fsspec_fs.exists(path):
            return {}
        children = fsspec_fs.ls(path, detail=False)
    else:
        if not os.path.exists(path):
            return {}
        children = [f"{path}/{child}" for child in os.listdir(path)]

    # Coalesce resource to None or a set of strings
    if isinstance(resource, str):
        resource = {resource}
    elif resource is not None:
        resource = set(resource)

    # Now grab filenames for all target resource types
    results = {}
    for child in sorted(children):  # sort for reproducibility
        results.update(_get_resource_type(child, resource, fsspec_fs=fsspec_fs))
    return results


def _get_resource_type(
    path: str,
    target_resources: Optional[set[str]],
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> dict[str, Optional[str]]:
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
    if pathlib.Path(path).suffix.casefold() not in {".jsonl", ".ndjson"}:
        return {}

    # Must be a regular file
    isfile_func = fsspec_fs.isfile if fsspec_fs else os.path.isfile
    if not isfile_func(path):
        return {}

    try:
        # Check just the first record, as all records in a file should be the same resource.
        # See https://www.hl7.org/fhir/R4/nd-json.html
        #
        # And since we cannot assume that "resourceType" is the first field,
        # we must parse the whole first line.
        # See https://www.hl7.org/fhir/R4/json.html#resources
        open_func = fsspec_fs.open if fsspec_fs else open
        with open_func(path, "r", encoding="utf8") as f:
            if not (line := f.readline()).rstrip("\r\n"):
                return {}
            parsed = json.loads(line)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Could not read from '%s': %s", path, str(exc))
        return {}

    resource_type = parsed.get("resourceType") if isinstance(parsed, dict) else None

    if target_resources is None or resource_type in target_resources:
        return {path: resource_type}

    # Didn't match our target resource types, just pretend it doesn't exist
    return {}


def read_multiline_json(
    path: PathType,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> Iterable[Any]:
    """
    Generator that yields lines of JSON from the target file.

    - I/O and JSON errors will be logged, not raised.
    - Will return an empty result if the path does not exist or is not readable.
    - The lines of JSON are not required to be dictionaries.

    :param path: the file to read
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :return: an iterable of parsed JSON results, line by line
    """
    path = str(path)
    open_func = fsspec_fs.open if fsspec_fs else open
    try:
        with open_func(path, "r", encoding="utf8") as f:
            for line_num, line in enumerate(f):
                if not line.rstrip("\r\n"):
                    # ignore empty lines (shouldn't normally happen,
                    # but maybe the file has an extra trailing new line
                    # or some other oddity - let's be graceful)
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning("Could not decode '%s:%d': %s", path, line_num + 1, str(exc))
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Could not read from '%s': %s", path, str(exc))


def read_multiline_json_from_dir(
    path: PathType,
    resource: ResourceType = None,
    *,
    fsspec_fs: Optional["fsspec.AbstractFileSystem"] = None,
) -> Iterable[Any]:
    """
    Generator that yields lines of JSON from the target folder.

    - This will not recurse into sub-folders.
    - I/O and JSON errors will be logged, not raised.
    - Will return an empty result if the path does not exist or is not readable.
    - Passing None as the resource filter (the default) will return all multi-line JSON found.
    - The lines of JSON are not required to be dictionaries.
    - The order of results will be consistent across calls.
    - This function will notice both JSON Lines (.jsonl) and NDJSON (.ndjson) files.

    :param path: the folder to scan
    :param resource: the type of FHIR resource(s) for which to return files
    :param fsspec_fs: optional fsspec FileSystem to use for I/O
    :return: an iterable of parsed JSON results, line by line
    """
    for filename in list_multiline_json_in_dir(path, resource, fsspec_fs=fsspec_fs):
        yield from read_multiline_json(filename, fsspec_fs=fsspec_fs)
