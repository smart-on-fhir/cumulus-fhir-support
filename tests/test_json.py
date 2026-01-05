"""Tests for ml_json.py"""

import contextlib
import gzip
import json
import os
import tempfile
import unittest
from collections.abc import Generator
from unittest import mock

import ddt
import fsspec

import cumulus_fhir_support as support


@ddt.ddt
class NdjsonTests(unittest.TestCase):
    """Test case for ndjson discovery and parsing"""

    @contextlib.contextmanager
    def assert_no_logs(self):
        # Back port of assertNoLogs from Python 3.10
        # Can drop this once we depend on 3.10+
        with mock.patch("cumulus_fhir_support.ml_json.logger") as mock_logger:
            yield
        self.assertEqual(0, mock_logger.error.call_count)
        self.assertEqual(0, mock_logger.warning.call_count)

    # ***************************
    # ** read_multiline_json() **
    # ***************************

    @ddt.data(
        (None, open),
        (".gz", gzip.open),
    )
    @ddt.unpack
    def test_read_happy_path(self, suffix, open_func):
        with tempfile.NamedTemporaryFile(suffix=suffix) as file:
            with open_func(file.name, "wt", encoding="utf8") as f:
                f.write('{"id": "2"}\n{"id": "1"}')
            with self.assert_no_logs():
                rows = support.read_multiline_json(file.name)
            self.assertIsInstance(rows, Generator)
            self.assertEqual([{"id": "2"}, {"id": "1"}], list(rows))

    def test_read_empty_lines_are_ignored(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "wb") as f:
                f.write(b'\r\n\n\n{"id": "1"}\r\n\n\r\n{"id": "2"}\n\n\r\n')
            with self.assert_no_logs():
                self.assertEqual(
                    [{"id": "1"}, {"id": "2"}], list(support.read_multiline_json(file.name))
                )

    def test_read_open_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertLogs("cumulus_fhir_support", level="ERROR") as cm:
                self.assertEqual([], list(support.read_multiline_json(f"{tmpdir}/not-here")))
                self.assertEqual(1, len(cm.output))
                self.assertTrue(
                    cm.output[0].startswith(
                        "ERROR:cumulus_fhir_support.ml_json:Could not read from"
                    ),
                    cm.output[0],
                )

    def test_read_decode_error(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
                f.write('{"id": "1"}\n{"id": "2" hello}\n{"id": "3"}')
            with self.assertLogs("cumulus_fhir_support", level="WARNING") as cm:
                self.assertEqual(
                    [
                        {"id": "1"},
                        {"id": "3"},
                    ],
                    list(support.read_multiline_json(file.name)),
                )
                self.assertEqual(1, len(cm.output))
                self.assertTrue(
                    cm.output[0].startswith(
                        "WARNING:cumulus_fhir_support.ml_json:Could not decode"
                    ),
                    cm.output[0],
                )

    def test_read_non_dict_is_fine(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
                f.write('1\n["2"]\n"3"')
            with self.assert_no_logs():
                rows = support.read_multiline_json(file.name)
            self.assertIsInstance(rows, Generator)
            self.assertEqual([1, ["2"], "3"], list(rows))

    # ****************************************
    # ** read_multiline_json_with_details() **
    # ****************************************

    def test_read_with_details_happy_path(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
                f.write('{"id": "1"}\n\n{"id": "2"}')
            with self.assert_no_logs():
                rows = support.read_multiline_json_with_details(file.name)
            self.assertIsInstance(rows, Generator)
            self.assertEqual(
                [
                    {"json": {"id": "1"}, "line_num": 0, "byte_offset": 0},
                    {"json": {"id": "2"}, "line_num": 2, "byte_offset": 13},
                ],
                list(rows),
            )

    def test_read_with_details_with_offset(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
                f.write('{"id": "1"}\n\n{"id": "2"}')
            with self.assert_no_logs():
                rows = list(support.read_multiline_json_with_details(file.name, offset=13))
            self.assertEqual([{"json": {"id": "2"}, "line_num": 0, "byte_offset": 0}], rows)

    def test_read_with_details_with_bad_offset(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
                f.write('{"id": "1"}\n\n{"id": "2"}')
            with self.assertLogs("cumulus_fhir_support", level="WARNING") as cm:
                rows = list(support.read_multiline_json_with_details(file.name, offset=5))
            self.assertEqual([{"json": {"id": "2"}, "line_num": 2, "byte_offset": 8}], rows)
            self.assertEqual(1, len(cm.output))
            self.assertTrue(
                cm.output[0].startswith("WARNING:cumulus_fhir_support.ml_json:Could not decode"),
                cm.output[0],
            )

    # **********************************
    # ** list_multiline_json_in_dir() **
    # **********************************

    @staticmethod
    def fill_dir(tmpdir: str, files: dict[str, list[dict]]):
        os.makedirs(tmpdir, exist_ok=True)
        for basename, content in files.items():
            open_func = open
            if basename.casefold().endswith(".gz"):
                open_func = gzip.open
            with open_func(f"{tmpdir}/{basename}", "wt", encoding="utf8") as f:
                for row in content:
                    json.dump(row, f)
                    f.write("\n")

    def test_list_any_ndjson_is_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "README.txt": [{"id": "ignored"}],
                    "file1.ndjson": [{"id": "file1", "resourceType": "Patient"}],
                    "file2.ndjson.gz": [{"id": "file2"}],
                },
            )
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(tmpdir)
            self.assertEqual(
                {
                    f"{tmpdir}/file1.ndjson": "Patient",
                    f"{tmpdir}/file2.ndjson.gz": None,
                },
                files,
            )

    def test_list_supports_multiple_formats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "file1.ndjson": [{"id": "NDJSON"}],
                    "file2.jsonl.Gz": [{"id": "JSON Lines"}],
                    "file3.JSONL": [{"id": "ignores case too"}],
                    "file3.txt.GZ": [{"id": "file3"}],
                },
            )
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(tmpdir)
            self.assertEqual(
                ["file1.ndjson", "file2.jsonl.Gz", "file3.JSONL"],
                [os.path.basename(p) for p in files],
            )

    @ddt.data(
        # target resources, expected answer
        (None, {"pat1", "pat2", "con", "obs", "none", "non-dict"}),
        ([], []),
        ("Patient", {"pat1", "pat2"}),
        ({"Condition", "Observation"}, {"con", "obs"}),
        (iter(["Condition", None]), {"con", "none", "non-dict"}),
    )
    @ddt.unpack
    def test_list_resource_filter(self, target_resources, expected_names):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "pat1.ndjson": [{"resourceType": "Patient"}],
                    "pat2.ndjson": [{"resourceType": "Patient"}],
                    "con.ndjson": [{"resourceType": "Condition"}],
                    "obs.ndjson": [{"resourceType": "Observation"}],
                    "none.ndjson": [{"id": "no-resource-type"}],
                    "non-dict.ndjson": [5, 6],
                    "empty.ndjson": [],
                    ".ndjson": [{"id": "ignored"}],
                    ".ndjson.gz": [{"id": "ignored"}],
                    ".gz": [{"id": "ignored"}],
                    "README.txt": [{"id": "ignored"}],
                },
            )
            os.mkdir(f"{tmpdir}/nope")
            os.mkdir(f"{tmpdir}/nope.ndjson")

            expected_types = {
                "pat1": "Patient",
                "pat2": "Patient",
                "con": "Condition",
                "obs": "Observation",
            }

            # Multiple resources
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(tmpdir, target_resources)
            self.assertIsInstance(files, dict)
            self.assertEqual(list(files.keys()), sorted(files.keys()))  # verify it's sorted

            expected_files = {
                f"{tmpdir}/{name}.ndjson": expected_types.get(name)
                for name in sorted(expected_names)
            }
            self.assertEqual(expected_files, files)

    @ddt.data(None, "local")
    def test_list_handles_missing_folder(self, fs_code):
        fs = fs_code and fsspec.filesystem(fs_code)
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(f"{tmpdir}/nope", fsspec_fs=fs)
            self.assertEqual({}, files)

    def test_list_decode_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/decode-error.ndjson", "w", encoding="utf8") as f:
                f.write("hello")
            with open(f"{tmpdir}/decode-success.ndjson", "w", encoding="utf8") as f:
                f.write('{"resourceType": "Patient"}')
            with self.assertLogs("cumulus_fhir_support", level="WARNING") as cm:
                files = support.list_multiline_json_in_dir(tmpdir)
            self.assertEqual(
                {
                    f"{tmpdir}/decode-success.ndjson": "Patient",
                },
                files,
            )
            self.assertEqual(1, len(cm.output))
            self.assertTrue(
                cm.output[0].startswith("WARNING:cumulus_fhir_support.ml_json:Could not read from"),
                cm.output[0],
            )

    @ddt.data(None, "local")
    def test_recursive_list(self, fs_code):
        fs = fs_code and fsspec.filesystem(fs_code)
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(f"{tmpdir}", {"external.ndjson": [{"id": "external"}]})
            self.fill_dir(f"{tmpdir}/external-dir", {"extern-sub.ndjson": [{"id": "extern-sub"}]})
            self.fill_dir(f"{tmpdir}/root", {"root.ndjson": [{"id": "root"}]})
            self.fill_dir(f"{tmpdir}/root/subdir", {"sub.ndjson": [{"id": "sub"}]})
            os.symlink("../external-dir", f"{tmpdir}/root/external-dir")  # should follow
            os.symlink("../external-link", f"{tmpdir}/root/outer.ndjson")  # should follow
            os.symlink("external.ndjson", f"{tmpdir}/external-link")  # should follow (again)

            # Confirm we iterate recursively if asked
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(
                    f"{tmpdir}/root", fsspec_fs=fs, recursive=True
                )
            self.assertEqual(
                list(files),
                [
                    f"{tmpdir}/external-dir/extern-sub.ndjson",
                    f"{tmpdir}/external.ndjson",
                    f"{tmpdir}/root/root.ndjson",
                    f"{tmpdir}/root/subdir/sub.ndjson",
                ],
            )

            # And once without the flag
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(f"{tmpdir}/root", fsspec_fs=fs)
            self.assertEqual(
                list(files),
                [
                    f"{tmpdir}/external.ndjson",
                    f"{tmpdir}/root/root.ndjson",
                ],
            )

    # ************************************
    # ** read_multiline_json_from_dir() **
    # ************************************

    @ddt.data(None, "local")
    def test_read_dir_happy_path(self, fs_code):
        fs = fs_code and fsspec.filesystem(fs_code)
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "pat.ndjson": [{"resourceType": "Patient", "id": "P1"}],
                    "con.ndjson": [
                        {"resourceType": "Condition", "id": "C1"},
                        {"resourceType": "Condition", "id": "C2"},
                    ],
                    "obs.ndjson.GZ": [{"resourceType": "Observation", "id": "O1"}],
                    "empty.ndjson": [],
                },
            )

            with self.assert_no_logs():
                rows = support.read_multiline_json_from_dir(
                    tmpdir, ["Condition", "Patient"], fsspec_fs=fs
                )
            self.assertIsInstance(rows, Generator)
            self.assertEqual(["C1", "C2", "P1"], [x["id"] for x in rows])
