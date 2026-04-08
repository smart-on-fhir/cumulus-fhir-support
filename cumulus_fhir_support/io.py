"""Abstract I/O access across network or local filesystems (using fsspec)"""

import contextlib
import functools
import json
import os
import pathlib
import typing
import urllib.parse

import fsspec

# Sentinel object to distinguish whether a default arg was provided.
# Once we depend on Python 3.13, we can use typing.NoDefault.
_NoDefault = object()


@functools.total_ordering
class FsPath:
    """A path into an fsspec filesystem.

    You should call FsPath.register_options() early, to register any filesystem-specific options
    that will be required to properly connect to remote filesystems like S3.

    Fsspec has some utility methods so that you don't need to keep a filesystem object around,
    which examine the URL schema, if any (like fsspec.open). But fsspec methods like list() will
    return paths without a schema prefix. So it's nice to have a reference to the filesystem
    around.
    """

    _options: typing.ClassVar = {}
    _fsspecs: typing.ClassVar = {}

    @classmethod
    def register_options(
        cls,
        *,
        endpoint_url: str | None = None,
        kms_key: str | None = None,
        region: str | None = None,
    ) -> None:
        # First, preserve these args for later inspection
        cls._options = {
            "endpoint_url": endpoint_url,
            "kms_key": kms_key,
            "region": region,
        }

        ### S3 ###
        s3 = {}

        # We want to enforce server side encryption, because FHIR data is sensitive.
        # Assume KMS encryption for now - we can make this tunable to AES256 if folks have a need.
        s3["s3_additional_kwargs"] = {"ServerSideEncryption": "aws:kms"}

        if endpoint_url:
            s3["endpoint_url"] = endpoint_url
        if kms_key:
            s3["s3_additional_kwargs"]["SSEKMSKeyId"] = kms_key
        if region:
            s3["client_kwargs"] = {"region_name": region}

        cls._fsspecs["s3"] = fsspec.filesystem("s3", **s3)

    @classmethod
    def get_registered_options(cls) -> dict[str, str | None]:
        return dict(cls._options)

    def __init__(
        self,
        *pathsegments: "str | pathlib.Path | FsPath",
        fs: fsspec.AbstractFileSystem | None = None,
    ):
        """Creates an FsPath from path segments.

        Pass in one or more path segments, optionally starting with a URL.

        If you have an existing FsPath as a base, pass that in too.
        """

        # Clean up the path
        self._path = os.path.join(*map(str, pathsegments))

        # Remember the protocol - we'll want to reference it later to make the FS object
        parsed = urllib.parse.urlparse(self._path)
        self._protocol = parsed.scheme or "file"

        # It's possible to use a custom fsspec instance (mostly for backwards compabitility
        # in some APIs that take an fsspec object)
        self._custom_fs = fs

    def copy(self, target: "FsPath") -> None:
        """Copies this FsPath source onto the target path."""
        if self.is_dir():
            raise NotImplementedError("Cannot copy a source directory.")
        if target.is_dir():
            raise NotImplementedError("Cannot copy onto a target directory.")

        with target.open("wb", compression=None) as out_file:
            with self.open("rb", compression=None) as in_file:
                while block := in_file.read(self.fs.blocksize):
                    out_file.write(block)

    def copy_into(self, target: "FsPath") -> None:
        """Copies this FsPath source into the target folder."""
        if not target.is_dir():
            raise NotImplementedError("Cannot copy into a file.")
        self.copy(target.joinpath(self.name))

    def exists(self) -> bool:
        return self.fs.exists(self._path)

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Returns the fsspec filesystem used for this path"""

        if self._custom_fs:
            return self._custom_fs

        # This lazily loads the filesystem object, which helps if options are registered after
        # this FsPath is created (notably - this allows argparse tricks like type=FsPath which
        # will be created before register_options() can normally be called)

        if fs := self._fsspecs.get(self._protocol):
            return fs

        fs = fsspec.filesystem(self._protocol)
        self._fsspecs[self._protocol] = fs
        return fs

    def is_dir(self) -> bool:
        """Returns True if this is a directory."""
        return self.fs.isdir(self._path)

    @property
    def is_http(self) -> bool:
        """
        Returns True if this is a http or https URL.

        This is useful for detecting user mistakes or or similar, since this won't be writable.
        (For example, providing a nicer error message if the user provides an EHR URL.)
        """
        return self._protocol in {"http", "https"}

    @property
    def is_local(self) -> bool:
        """Returns True if this is a local file path."""
        return self._protocol == "file"

    def joinpath(self, *pathsegments: str | pathlib.Path) -> "FsPath":
        return FsPath(self._path, *pathsegments)

    def ls(self, *, include_dirs: bool = True, recursive: bool = False) -> set["FsPath"]:
        """
        List all children, optionally recursively.

        Will follow symlinks.

        You can avoid returning any directories with "include_dirs=False".
        """
        return {
            FsPath(self._full(path))
            for path in self._walk_tree(self._path, include_dirs=include_dirs, recursive=recursive)
        }

    def makedirs(self) -> None:
        if self._protocol == "s3":
            # s3 doesn't really care about folders, and if we try to make one,
            # fsspec would want the CreateBucket permission as it goes up the tree
            return
        self.fs.makedirs(self._path, exist_ok=True)

    @property
    def name(self) -> str:
        return os.path.basename(self._path)

    @contextlib.contextmanager
    def open(self, mode: str = "r", **kwargs) -> typing.IO:
        """
        Opens the file.

        Writes are atomic by default (the target path will be unaffected until the open is closed).
        Compression will be automatically inferred based on suffix.
        """

        with contextlib.ExitStack() as stack:
            if "w" in mode:
                # fsspec is atomic per-transaction.
                # If an error occurs inside the transaction, partial writes will be discarded.
                # But we only want a transaction if we're writing - read transactions can error out
                stack.enter_context(self.fs.transaction)

            yield stack.enter_context(self.open_direct(mode, **kwargs))

    def open_direct(self, mode: str = "r", **kwargs) -> typing.IO:
        """
        Opens the file directly, without a context manager.

        You must manually call close() yourself.
        The operation will not be atomic.
        """
        # allow callers to override these defaults if they want
        kwargs.setdefault("compression", "infer")
        kwargs.setdefault("encoding", "utf8")
        return self.fs.open(self._path, mode=mode, **kwargs)

    @property
    def parent(self) -> "FsPath":
        return FsPath(self._full(os.path.dirname(self._path)))

    def relative_to(self, other: "FsPath") -> str:
        """If not relative to each other, returns the full path for self"""
        return str(self).removeprefix(str(other)).lstrip("/")

    def rm(self) -> None:
        self.fs.rm(self._path, recursive=True)

    @property
    def suffix(self) -> str:
        """Returns the last suffix (including the period)"""
        return pathlib.Path(self._path).suffix

    @property
    def suffixes(self) -> list[str]:
        """Returns a list of all suffixes (including the periods)"""
        return pathlib.Path(self._path).suffixes

    def __str__(self):
        return self._full(self._path)

    def __repr__(self):
        return f'FsPath("{self}")'

    def __rich__(self) -> str:
        """Lets this be rendered directly by rich"""
        return str(self)

    def __eq__(self, other: "FsPath | None") -> bool:
        if other is None:
            return False
        return str(self) == str(other)

    def __lt__(self, other: "FsPath") -> bool:
        if not isinstance(other, FsPath):
            return NotImplemented
        return str(self) < str(other)

    def __hash__(self) -> int:
        return hash(str(self))

    ###############################################
    # Convenience read/write methods
    ###############################################

    def read_bytes(self, *, default=_NoDefault) -> bytes:
        try:
            with self.open("rb") as f:
                return f.read()
        except Exception:
            if default is not _NoDefault:
                return default
            raise

    def read_json(self, *, default=_NoDefault) -> object:
        try:
            with self.open() as f:
                return json.load(f)
        except Exception:
            if default is not _NoDefault:
                return default
            raise

    def read_text(self, *, default=_NoDefault) -> str:
        try:
            with self.open() as f:
                return f.read()
        except Exception:
            if default is not _NoDefault:
                return default
            raise

    def write_bytes(self, content: bytes, /) -> None:
        with self.open("wb") as f:
            f.write(content)

    def write_json(self, content: object, /, *, indent: int | None = None) -> None:
        self.write_text(json.dumps(content, indent=indent))

    def write_text(self, content: str, /) -> None:
        with self.open("w") as f:
            f.write(content)

    ###############################################
    # Private helpers
    ###############################################

    def _full(self, path: str) -> str:
        if self.is_local:
            return path
        else:
            return self.fs.unstrip_protocol(path)

    def _walk_tree(
        self,
        path: str,
        *,
        include_dirs: bool,
        recursive: bool,
        visited: set[str] | None = None,
    ) -> set[str]:
        if not self.fs.exists(path):
            return set()
        visited = visited or set()
        results = set()

        if recursive:
            items = self.fs.find(path, detail=True).values()
        elif visited:
            items = [self.fs.info(path)]  # no iteration as we follow links
        else:
            items = self.fs.ls(path, detail=True)

        for details in items:
            full = details["name"]
            if details.get("islink") and details.get("destination"):
                resolved = os.path.join(os.path.dirname(full), details.get("destination"))
                resolved = os.path.normpath(resolved)
                was_visited = resolved in visited
                visited.add(resolved)
                if not was_visited:
                    results |= self._walk_tree(
                        resolved,
                        include_dirs=include_dirs,
                        recursive=recursive,
                        visited=visited,
                    )
            elif details.get("type") == "file":
                results.add(full)
            elif details.get("type") == "directory" and include_dirs:
                results.add(full)
        return results
