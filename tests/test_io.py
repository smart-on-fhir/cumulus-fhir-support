"""Tests for io.py"""

import contextlib
import itertools
import pathlib
import tempfile
import unittest
from unittest import mock

import ddt
import fsspec

import cumulus_fhir_support as cfs


@ddt.ddt
class IoTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        cfs.FsPath.register_options()  # reset options

    def test_basics(self):
        """Test all the basic non-read/write stuff"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = cfs.FsPath(tmpdir)
            child_dir = tmp.joinpath("child_dir")
            child_dir.makedirs()
            child_file = tmp.joinpath("child.txt")
            child_file.write_text("hello")
            nope = cfs.FsPath(tmp, "nope", pathlib.Path("child"))
            abs_child = cfs.FsPath(tmp, "/child")
            rel = cfs.FsPath("rel")
            s3 = cfs.FsPath("s3://bucket/file.txt.gz")
            http = cfs.FsPath("http://server/file.txt.gz")
            home = cfs.FsPath("/home/me")

            self.assertTrue(tmp.exists())
            self.assertFalse(nope.exists())

            self.assertIs(nope.fs, tmp.fs)  # filesystems are shared
            self.assertIsNot(nope.fs, s3.fs)  # unless they use different protocols

            self.assertTrue(child_dir.is_dir())
            self.assertFalse(child_file.is_dir())
            self.assertFalse(nope.is_dir())

            self.assertTrue(http.is_http)
            self.assertFalse(s3.is_http)

            self.assertTrue(tmp.is_local)
            self.assertFalse(s3.is_local)

            self.assertEqual(nope.name, "child")
            self.assertEqual(child_dir.name, "child_dir")
            self.assertEqual(http.name, "file.txt.gz")

            self.assertEqual(http.parent, "http://server")
            self.assertEqual(http.parent.parent, "http://server")
            self.assertEqual(home.parent, "/home")
            self.assertEqual(home.parent.parent, "/")
            self.assertEqual(home.parent.parent.parent, "/")
            self.assertEqual(rel.parent, ".")
            self.assertEqual(rel.parent.parent, ".")
            self.assertEqual(cfs.FsPath("file://rel").parent, ".")
            self.assertEqual(cfs.FsPath("file://rel/child").parent, "rel")
            self.assertEqual(cfs.FsPath("file:///home").parent, "/")
            self.assertEqual(cfs.FsPath("file:///home/me").parent, "/home")

            self.assertEqual(nope.relative_to(tmp), "nope/child")
            self.assertEqual(child_dir.relative_to(tmp), "child_dir")
            self.assertEqual(child_dir.relative_to(home), child_dir)
            self.assertEqual(tmp.relative_to(tmp), ".")

            self.assertEqual(nope.stem, "child")
            self.assertEqual(s3.stem, "file.txt")
            self.assertEqual(child_file.stem, "child")

            self.assertEqual(nope.suffix, "")
            self.assertEqual(s3.suffix, ".gz")
            self.assertEqual(s3.suffixes, [".txt", ".gz"])

            self.assertEqual(str(rel), "rel")
            self.assertEqual(str(home), "/home/me")
            self.assertEqual(str(s3), "s3://bucket/file.txt.gz")
            self.assertEqual(str(abs_child), "/child")

            self.assertEqual(repr(rel), 'FsPath("rel")')

            self.assertEqual(rel, "rel")
            self.assertEqual(rel, pathlib.Path("rel"))
            self.assertEqual(rel, cfs.FsPath("rel"))
            self.assertNotEqual(rel, nope)
            self.assertNotEqual(rel, None)

            self.assertGreater(rel, "aaa")
            self.assertLess(rel, "zzz")

            self.assertEqual(hash(rel), hash("rel"))

            self.assertEqual(rel.__rich__(), "rel")

    def test_s3(self):
        """Test all the s3 specific stuff and registered options"""
        s3 = cfs.FsPath("s3://bucket/file.txt.gz")

        options = {"endpoint_url": "yup", "kms_key": "kms", "region": "region"}
        cfs.FsPath.register_options(**options)

        # Confirm we can get and modify registered options and it doesn't affect class's version
        reg_options = cfs.FsPath.get_registered_options()
        reg_options["kms_key"] = "blarg"
        self.assertEqual(cfs.FsPath.get_registered_options(), options)

        s3.makedirs()  # just confirm it doesn't blow up, since we skip it on S3

        # Confirm the filesystem used got the options
        self.assertEqual(s3.fs.endpoint_url, "yup")
        self.assertEqual(s3.fs.client_kwargs, {"region_name": "region"})
        self.assertEqual(
            s3.fs.s3_additional_kwargs, {"SSEKMSKeyId": "kms", "ServerSideEncryption": "aws:kms"}
        )

    def test_custom_fs(self):
        fs = fsspec.filesystem("s3")
        s3 = cfs.FsPath("bucket/file.txt", fs=fs)

        self.assertFalse(s3.is_local)
        self.assertEqual(str(s3), "s3://bucket/file.txt")
        self.assertIs(s3.fs, fs)

    def test_rm(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = cfs.FsPath(tmpdir, "1/2/3/4/5/file.txt")
            path.parent.makedirs()

            path.write_text("hello")
            self.assertTrue(path.exists())

            path.rm()
            self.assertFalse(path.exists())
            self.assertTrue(path.parent.exists())

            cfs.FsPath(tmpdir, "1/2").rm()
            self.assertEqual(cfs.FsPath(tmpdir, "1").ls(), set())

            with self.assertRaises(FileNotFoundError):
                cfs.FsPath(tmpdir, "nope").rm()

    def test_read_write_utils(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = cfs.FsPath(tmpdir, "file")

            path.write_json({"hello": "world"}, indent=2)
            self.assertEqual(path.read_json(), {"hello": "world"})
            self.assertEqual(path.read_text(), '{\n  "hello": "world"\n}')
            self.assertEqual(path.read_bytes(), b'{\n  "hello": "world"\n}')

            path.write_text('"hello"')
            self.assertEqual(path.read_json(), "hello")
            self.assertEqual(path.read_text(), '"hello"')
            self.assertEqual(path.read_bytes(), b'"hello"')

            path.write_bytes(b"\xc3\x28")
            with self.assertRaises(UnicodeDecodeError):
                path.read_json()
            with self.assertRaises(UnicodeDecodeError):
                path.read_text()
            self.assertEqual(path.read_bytes(), b"\xc3\x28")

            path.rm()

            with self.assertRaises(FileNotFoundError):
                path.read_json()
            with self.assertRaises(FileNotFoundError):
                path.read_text()
            with self.assertRaises(FileNotFoundError):
                path.read_bytes()

            self.assertEqual(path.read_json(default="missing"), "missing")
            self.assertEqual(path.read_text(default=404), 404)
            self.assertEqual(path.read_bytes(default=["gone fishing"]), ["gone fishing"])

    def test_copy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = cfs.FsPath(tmpdir, "src")
            src_dir.makedirs()
            src_file = src_dir.joinpath("src.txt")
            src_file.write_text("hello")
            src_subdir = src_dir.joinpath("subdir")
            src_subdir.makedirs()
            src_sub_file = src_subdir.joinpath("sub.txt")
            src_sub_file.write_text("bye")
            dst_dir = cfs.FsPath(tmpdir, "dst")
            dst_file = dst_dir.joinpath("dst.txt")

            # Copying onto yourself is allowed
            src_file.copy(src_file)
            self.assertEqual(src_file.read_text(), "hello")

            # Single file copy
            src_file.copy(dst_file)
            self.assertEqual(dst_file.read_text(), "hello")

            # Tree copy
            tree = dst_dir.joinpath("treecopy")
            src_dir.copy(tree)
            self.assertTrue(tree.joinpath("subdir").is_dir())
            self.assertTrue(tree.joinpath("src.txt").read_text(), "hello")
            self.assertTrue(tree.joinpath("subdir/sub.txt").read_text(), "bye")

            # Single file copy_into
            target = src_file.copy_into(dst_dir)
            self.assertEqual(target.read_text(), "hello")
            self.assertEqual(target, dst_dir.joinpath("src.txt"))

            # Tree copy_into
            target = src_subdir.copy_into(dst_dir)
            self.assertEqual(target.joinpath("sub.txt").read_text(), "bye")
            self.assertEqual(target, dst_dir.joinpath("subdir"))

    @contextlib.contextmanager
    def exploding_text(self):
        """
        Yields text data that will raise an error after some but not all data has been written"""

        orig_open = open
        orig_write = None

        def exploding_write(*args, **kwargs):
            orig_write(*args, **kwargs)
            raise KeyboardInterrupt

        def open_wrapper(*args, **kwargs):
            nonlocal orig_write
            opened_file = orig_open(*args, **kwargs)
            orig_write = opened_file.write
            opened_file.write = exploding_write
            return opened_file

        with mock.patch("builtins.open", new=open_wrapper):
            with self.assertRaises(KeyboardInterrupt):
                yield "1" * (fsspec.spec.AbstractBufferedFile.DEFAULT_BLOCK_SIZE + 1)

    def test_writes_are_atomic(self):
        """Verify that our write utilities are atomic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = cfs.FsPath(tmpdir)

            # Try a couple of our write methods, confirm that nothing makes it through
            with self.exploding_text() as text:
                root.joinpath("atomic.txt").write_text(text)
            with self.exploding_text() as text:
                root.joinpath("atomic.json").write_json({"hello": text})
            self.assertEqual(root.ls(), set())

            # By default, fsspec writes are not atomic - just sanity check that text _can_ get
            # through exploding_text
            with self.exploding_text() as text:
                partial_path = str(root.joinpath("partial.txt"))
                with root.fs.open(partial_path, "w") as f:
                    f.write(text)
            self.assertEqual(root.ls(), {partial_path})

    @ddt.idata(
        # Every combination of these sizes, backends, and data formats:
        itertools.product(
            [5, fsspec.spec.AbstractBufferedFile.DEFAULT_BLOCK_SIZE + 1],
            ["local"],  # should add S3FS testing here at some point
            ["json", "text", "bytes"],
        )
    )
    @ddt.unpack
    def test_writes_happy_path(self, size, backend, data_format):
        """
        Verify that writes of various sizes and formats are written out correctly.

        This may seem paranoid, but we've seen fsspec not write them out inside a transaction,
        because we forgot to close or flush the file.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            match backend:
                case "local":
                    path = cfs.FsPath(tmpdir, "file.txt")
                case _:
                    raise ValueError

            match data_format:
                case "text":
                    write = path.write_text
                    read = path.read_text
                    data = "1" * size
                case "json":
                    write = path.write_json
                    read = path.read_json
                    data = ["1" * size]
                case "bytes":
                    write = path.write_bytes
                    read = path.read_bytes
                    data = b"1" * size
                case _:
                    raise ValueError

            write(data)
            result = read()

        self.assertEqual(data, result)
