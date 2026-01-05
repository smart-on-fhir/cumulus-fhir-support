"""Tests for http.py"""

import datetime
import unittest
from unittest import mock

import ddt
import httpx
import respx
import time_machine

import cumulus_fhir_support as cfs


@ddt.ddt
class HttpTests(unittest.IsolatedAsyncioTestCase):
    """
    Test case for http utility methods
    """

    def setUp(self):
        super().setUp()

        # Initialize responses mock
        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

    @ddt.data(True, False)  # confirm that we handle both stream and non-stream resets
    async def test_get_error_401(self, stream_mode):
        """Verify that we call the auth callback."""
        route = self.respx_mock.get("http://example.com/")
        route.side_effect = [httpx.Response(401), httpx.Response(200)]

        auth_callback = mock.AsyncMock(return_value={})
        response = await cfs.http_request(
            httpx.AsyncClient(),
            "GET",
            "http://example.com/",
            stream=stream_mode,
            auth_callback=auth_callback,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(auth_callback.call_count, 1)

    async def test_get_error_401_persistent_error(self):
        """Verify that we surface persistent errors during auth."""
        self.respx_mock.get("http://example.com/").respond(status_code=401)

        auth_callback = mock.AsyncMock(return_value={})
        with self.assertRaises(cfs.FatalNetworkError) as cm:
            await cfs.http_request(
                httpx.AsyncClient(),
                "GET",
                "http://example.com/",
                retry_delays=[],
                auth_callback=auth_callback,
            )
        self.assertEqual(cm.exception.response.status_code, 401)
        self.assertEqual(auth_callback.call_count, 1)

    @ddt.data(
        # OperationOutcome
        {
            "json": {
                "resourceType": "OperationOutcome",
                "issue": [{"diagnostics": "testmsg"}],
            }
        },
        # non-OperationOutcome json
        {"json": {"issue": [{"diagnostics": "msg"}]}, "extensions": {"reason_phrase": b"testmsg"}},
        {"text": "testmsg"},  # just pure text content
        {"json": '"testmsg"'},  # json, but just a text message
        {"extensions": {"reason_phrase": b"testmsg"}},
        {"json": {"error_description": "testmsg"}},
        {"json": {"error_uri": "http://testmsg.com/"}},
    )
    async def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        self.respx_mock.get("http://example.com/").mock(
            return_value=httpx.Response(400, **response_args),
        )
        with self.assertRaisesRegex(cfs.FatalNetworkError, "testmsg"):
            await cfs.http_request(httpx.AsyncClient(), "GET", "http://example.com/")

    @ddt.data(
        (None, 120),  # default to the caller's retry delay
        ("10", 10),  # parse simple integers
        ("Tue, 14 Sep 2021 21:23:58 GMT", 13),  # parse http-dates too
        ("abc", 120),  # if parsing fails, use caller's retry delay
        ("-5", 0),  # floor of zero
        ("Mon, 13 Sep 2021 21:23:58 GMT", 0),  # floor of zero on dates too
    )
    @ddt.unpack
    @time_machine.travel(
        datetime.datetime(
            2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4))
        ),
        tick=False,
    )
    async def test_parse_retry_after(self, retry_after_header, expected_delay):
        headers = {"Retry-After": retry_after_header} if retry_after_header else {}
        response = httpx.Response(200, headers=headers)
        self.assertEqual(cfs.parse_retry_after(response, 120), expected_delay)

    @ddt.data(
        # status, expect_retry
        (300, False),
        (400, False),
        (408, True),
        (429, True),
        (500, True),
        (501, False),
        (502, True),
        (503, True),
        (504, True),
    )
    @ddt.unpack
    @mock.patch("asyncio.sleep")
    async def test_retry_codes(self, status_code, expect_retry, sleep_mock):
        self.respx_mock.get("http://example.com/").respond(status_code=status_code)

        with self.assertRaises(cfs.NetworkError) as cm:
            await cfs.http_request(
                httpx.AsyncClient(), "GET", "http://example.com/", retry_delays=[1]
            )

        self.assertEqual(sleep_mock.call_count, 1 if expect_retry else 0)
        self.assertIsInstance(
            cm.exception,
            cfs.TemporaryNetworkError if expect_retry else cfs.FatalNetworkError,
        )

    @mock.patch("httpx.AsyncClient.send")
    @mock.patch("asyncio.sleep")
    async def test_invalid_dns(self, mock_sleep, mock_send):
        """Verify that random pre-response errors bubble up as FatalErrors."""
        mock_send.side_effect = httpx.ConnectTimeout("Connect timeout")
        with self.assertRaisesRegex(cfs.TemporaryNetworkError, "Connect timeout") as cm:
            await cfs.http_request(httpx.AsyncClient(), "GET", "http://example.invalid/")
        self.assertIsNone(cm.exception.response)
