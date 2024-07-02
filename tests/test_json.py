"""Tests for ndjson.py"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from collections.abc import Generator
from unittest import mock

import ddt

import cumulus_fhir_support as support


@ddt.ddt
class NdjsonTests(unittest.TestCase):
    """Test case for ndjson discovery and parsing"""

    @contextlib.contextmanager
    def assert_no_logs(self):
        # Back port of assertNoLogs from Python 3.10
        # Can drop this once we depend on 3.10+
        with mock.patch("cumulus_fhir_support.json.logger") as mock_logger:
            yield
        self.assertEqual(0, mock_logger.error.call_count)
        self.assertEqual(0, mock_logger.warning.call_count)

    # ***************************
    # ** read_multiline_json() **
    # ***************************

    def test_read_happy_path(self):
        with tempfile.NamedTemporaryFile() as file:
            with open(file.name, "w", encoding="utf8") as f:
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
                    cm.output[0].startswith("ERROR:cumulus_fhir_support.json:Could not read from"),
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
                    cm.output[0].startswith("WARNING:cumulus_fhir_support.json:Could not decode"),
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

    # **********************************
    # ** list_multiline_json_in_dir() **
    # **********************************

    @staticmethod
    def fill_dir(tmpdir: str, files: dict[str, list[dict]]):
        for basename, content in files.items():
            with open(f"{tmpdir}/{basename}", "w", encoding="utf8") as f:
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
                    "file2.ndjson": [{"id": "file2"}],
                },
            )
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(tmpdir)
            self.assertEqual(
                {
                    f"{tmpdir}/file1.ndjson": "Patient",
                    f"{tmpdir}/file2.ndjson": None,
                },
                files,
            )

    def test_list_supports_multiple_formats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "file1.ndjson": [{"id": "NDJSON"}],
                    "file2.jsonl": [{"id": "JSON Lines"}],
                    "file3.JSONL": [{"id": "ignores case too"}],
                    "file3.txt": [{"id": "file3"}],
                },
            )
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(tmpdir)
            self.assertEqual(
                ["file1.ndjson", "file2.jsonl", "file3.JSONL"], [os.path.basename(p) for p in files]
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

    def test_list_handles_missing_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assert_no_logs():
                files = support.list_multiline_json_in_dir(f"{tmpdir}/nope")
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
                cm.output[0].startswith("WARNING:cumulus_fhir_support.json:Could not read from"),
                cm.output[0],
            )

    # ************************************
    # ** read_multiline_json_from_dir() **
    # ************************************

    def test_read_dir_happy_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.fill_dir(
                tmpdir,
                {
                    "pat.ndjson": [{"resourceType": "Patient", "id": "P1"}],
                    "con.ndjson": [
                        {"resourceType": "Condition", "id": "C1"},
                        {"resourceType": "Condition", "id": "C2"},
                    ],
                    "obs.ndjson": [{"resourceType": "Observation", "id": "O1"}],
                    "empty.ndjson": [],
                },
            )

            with self.assert_no_logs():
                rows = support.read_multiline_json_from_dir(tmpdir, ["Condition", "Patient"])
            self.assertIsInstance(rows, Generator)
            self.assertEqual(["C1", "C2", "P1"], [x["id"] for x in rows])

    # *******************
    # ** Miscellaneous **
    # *******************

    def test_fsspec_support(self):
        fake_files = ["folder/1.ndjson", "folder/nope"]
        fake_folders = ["folder/dir.ndjson"]
        all_fakes = fake_files + fake_folders

        def fake_ls(folder, detail):
            self.assertEqual(folder, "folder")
            self.assertFalse(detail)
            return all_fakes

        def fake_open(filename, mode, encoding):
            self.assertEqual(filename, "folder/1.ndjson")
            self.assertEqual(mode, "r")
            self.assertEqual(encoding, "utf8")
            return io.StringIO(
                '{"id": "P2", "resourceType": "Patient"}\n'
                '{"id": "P1", "resourceType": "Patient"}\n'
            )

        mock_fs = mock.Mock()
        mock_fs.exists = lambda x: x == "folder" or x in all_fakes
        mock_fs.isfile = lambda x: x in fake_files
        mock_fs.ls = fake_ls
        mock_fs.open = fake_open

        with self.assert_no_logs():
            rows = support.read_multiline_json_from_dir("folder", "Patient", fsspec_fs=mock_fs)
        self.assertEqual(["P2", "P1"], [x["id"] for x in rows])
