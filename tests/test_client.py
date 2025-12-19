"""Tests for FhirClient and similar"""

import datetime
import tempfile
import time
import unittest
from unittest import mock

import ddt
import httpx
import respx
import time_machine
from jwcrypto import jwk, jwt

import cumulus_fhir_support as cfs


@ddt.ddt
@mock.patch("uuid.uuid4", new=lambda: "1234")
@time_machine.travel(
    datetime.datetime(
        2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4))
    ),
    tick=False,
)
class TestFhirClient(unittest.IsolatedAsyncioTestCase):
    """
    Test case for FHIR client oauth2 / request support.

    i.e. tests for fhir_client.py
    """

    def setUp(self):
        super().setUp()

        # By default, set up a working server and auth. Tests can break things as needed.

        self.client_id = "my-client-id"
        self.jwk = jwk.JWK.generate(
            kty="RSA", alg="RS384", kid="a", key_ops=["sign", "verify"]
        ).export(as_dict=True)
        self.jwks = {"keys": [self.jwk]}
        self.server_url = "https://example.com/fhir"
        self.token_url = "https://auth.example.com/token"

        # Generate an example PEM file too (ES256)
        self.pem = """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIBd9Cq6RyRFloYDH5svVm53zSZnWC5VNp7E/ZbZ+17VVoAoGCCqGSM49
AwEHoUQDQgAE4DGrth4me9cwOxxDEYrWgzfQpdQud0twEz6CdIP0v+uSBeg+RhjF
4g2BoVmp8TwZoz7myMqTT4tWi+V9i97T7w==
-----END EC PRIVATE KEY-----"""

        # Initialize responses mock
        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

        # We ask for smart-configuration to discover the token endpoint
        self.smart_configuration = {
            "capabilities": ["client-confidential-asymmetric"],
            "token_endpoint": self.token_url,
            "token_endpoint_auth_methods_supported": ["private_key_jwt"],
        }
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        # simple capabilities (no vendor quirks) by default
        self.respx_mock.get(f"{self.server_url}/metadata").respond(json={"publisher": "SMART"})

        self.respx_mock.post(
            self.token_url,
            name="token",
        ).respond(json={"access_token": "1234"})

    async def test_required_arguments(self):
        """Verify that we require both a client ID and a JWK Set"""
        # Deny any actual requests during this test
        self.respx_mock.get(f"{self.server_url}/test").respond(status_code=401)

        # Simple helper to open and make a call on client.
        async def use_client(request=False, code=None, url=self.server_url, **kwargs):
            async with cfs.FhirClient(url, [], **kwargs) as client:
                if request:
                    await client.request("GET", "test")

        # No SMART args at all doesn't cause any problem, if we don't make calls
        await use_client()

        # No SMART args at all will raise though if we do make a call
        with self.assertRaises(cfs.BadAuthArguments):
            await use_client(request=True)

        # No base URL
        with self.assertRaises(cfs.BadAuthArguments):
            await use_client(request=True, url=None)

        # No JWKS or PEM
        with self.assertRaises(cfs.BadAuthArguments):
            await use_client(smart_client_id="foo")

        # No client ID (only JWKS)
        with self.assertRaises(cfs.BadAuthArguments):
            await use_client(smart_jwks=self.jwks)

        # No client ID (only PEM)
        with self.assertRaises(cfs.BadAuthArguments):
            await use_client(smart_pem=self.pem)

        # Works fine if both given
        await use_client(smart_client_id="foo", smart_jwks=self.jwks)
        await use_client(smart_client_id="foo", smart_pem=self.pem)

    @ddt.data(True, False)
    async def test_auth_with_jwks(self, scopes_v2):
        """Verify that we authorize JWKS correctly upon class initialization"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer 1234"},  # the same access token used in setUp()
        )

        # Add test capabilities
        if scopes_v2:
            self.smart_configuration["capabilities"].append("permission-v2")
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        async with cfs.FhirClient(
            self.server_url,
            ["Condition", "Patient"],
            smart_client_id=self.client_id,
            smart_jwks=self.jwks,
        ) as client:
            await client.request("GET", "foo")

        # Generate expected JWT
        token = jwt.JWT(
            header={
                "alg": "RS384",
                "kid": "a",
                "typ": "JWT",
            },
            claims={
                "iss": self.client_id,
                "sub": self.client_id,
                "aud": self.token_url,
                "exp": int(time.time()) + 299,  # aided by time-machine not changing time under us
                "jti": "1234",
            },
        )
        token.make_signed_token(key=jwk.JWK(**self.jwk))
        expected_jwt = token.serialize()

        # Check that we asked for a token & we included all the right params
        scope = "rs" if scopes_v2 else "read"
        self.assertEqual(1, self.respx_mock["token"].call_count)
        self.assertEqual(
            "&".join(
                [
                    "grant_type=client_credentials",
                    f"scope=system%2FCondition.{scope}+system%2FPatient.{scope}",
                    "client_assertion_type="
                    "urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
                    f"client_assertion={expected_jwt}",
                ]
            ),
            self.respx_mock["token"].calls.last.request.content.decode("utf8"),
        )

    async def test_auth_with_pem(self):
        """Verify that we authorize PEM correctly upon class initialization"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer 1234"},  # the same access token used in setUp()
        )

        async with cfs.FhirClient(
            self.server_url,
            ["Condition", "Patient"],
            smart_client_id=self.client_id,
            smart_pem=self.pem,
        ) as client:
            await client.request("GET", "foo")

        # Check that we asked for a token
        self.assertEqual(1, self.respx_mock["token"].call_count)

        # Don't bother checking that the JWT is what we expect, since the JWT changes every time
        # we generate one, with a curved algorithm.

    async def test_invalid_pem(self):
        """Verify that a bogus PEM is caught"""
        with self.assertRaises(cfs.BadAuthArguments):
            async with cfs.FhirClient(
                self.server_url,
                ["Patient"],
                smart_client_id=self.client_id,
                smart_pem="hello world!",
            ) as client:
                await client.request("GET", "foo")

    @ddt.data(
        # These private keys are all generated just for this test, they are not production keys.
        (
            "RS384",
            """-----BEGIN PRIVATE KEY-----
MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEA4wkL5iXvx5apo/Wk
8Z5KPfwTYYI6FG+lkjrYdVO+MW6WyEYNV7lx7wvPXXeJxBws/UXbAqo+qMxotIhn
VyPdrQIDAQABAkEAzlVFbCfkMEcb63fvLOvH22eBkafR8wq4thom6RJvkumRYaow
6H7hx8A9XLcVdiJUYhZSgd7pRd2MewG5Hr1ZoQIhAPeFUZwiGrLKB62VQtWHgub5
e16zbwUBAaF+NpjTxYglAiEA6tAToZTvnU+43OaR9IbnygrhO6KLUM9ygIOwZQ6S
5OkCIE1ZXCdugOleOQgFnN0de8qyK9tsN0VZCylsR6N6ikABAiBIXv9d8tBzVMnu
U6Yyjo3MKNRIlA2KR5XL5EquqvI9WQIgJiD71zLVteurFGXlMnKVwzGtVugShVHh
sr/xXbgncyc=
-----END PRIVATE KEY-----""",
        ),
        (
            "ES256",
            """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIBd9Cq6RyRFloYDH5svVm53zSZnWC5VNp7E/ZbZ+17VVoAoGCCqGSM49
AwEHoUQDQgAE4DGrth4me9cwOxxDEYrWgzfQpdQud0twEz6CdIP0v+uSBeg+RhjF
4g2BoVmp8TwZoz7myMqTT4tWi+V9i97T7w==
-----END EC PRIVATE KEY-----""",
        ),
        (
            "ES384",
            """-----BEGIN PRIVATE KEY-----
MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDCuAVir5v+2gZv7A3dR
EKT977pKPY+1S+h58Xbzir2gqdUKLuyCUCYJmQ6/7ac4B4ShZANiAASndkjwMCbG
fwEnf3fpjkwdEtdMCDpLEI2G4fokES6J66JxRj3CpmTwLrdJkiPiG0B6pKO+zVft
4j1XajyxhSmyuPpZQo7KaoW2QLEzBZC4M+1ko4cLd9JaSNC9//vcYf4=
-----END PRIVATE KEY-----""",
        ),
        (
            "ES512",
            """-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIAH/lTLRHRetOZo+nzJNZmxSPSrfC53q8M8aAwVoWTj6b+6gFUDqC1
+bWoioCDphoT6GgFK3ns/IuXbbDWrtzYafKgBwYFK4EEACOhgYkDgYYABABhNHfr
CPNIdf9jnnhh1nj6FqqNQ/Q0nbfc/LAi2p6fAmsAUr8lE/TafmAyhgbElFvS6IhF
ArPf/3aPaI5x73XzTgB0/01Y+2n+Kp6tRwgS08Uvv0AqS5Xw/xAo7fLID5IqoWg5
IRxyq6i4LnRleQHDKzI0hdZJPEQd3k3RsPC9IsBf0A==
-----END EC PRIVATE KEY-----""",
        ),
    )
    @ddt.unpack
    @mock.patch("jwcrypto.jwt.JWT")
    async def test_pem_types(self, expected_alg, pem, mock_jwt):
        """Verify that support several PEM algorithm types"""
        mock_jwt.side_effect = ZeroDivisionError

        with self.assertRaises(ZeroDivisionError):
            async with cfs.FhirClient(
                self.server_url,
                ["Condition"],
                smart_client_id=self.client_id,
                smart_pem=pem,
            ) as client:
                await client.request("GET", "foo")

        self.assertEqual(mock_jwt.call_count, 1)
        self.assertEqual(mock_jwt.call_args[1]["header"]["alg"], expected_alg)

    async def test_auth_with_bearer_token(self):
        """Verify that we pass along the bearer token to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer fob"},
        )

        async with cfs.FhirClient(
            self.server_url, ["Condition", "Patient"], bearer_token="fob"
        ) as server:
            await server.request("GET", "foo")

    async def test_auth_with_basic_auth(self):
        """Verify that we pass along the basic user/password to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Basic VXNlcjpwNHNzdzByZA=="},
        )

        async with cfs.FhirClient(
            self.server_url, [], basic_user="User", basic_password="p4ssw0rd"
        ) as server:
            await server.request("GET", "foo")

    async def test_auth_with_partial_basic_auth(self):
        """Verify that we fail if only some basic auth args are given"""
        with self.assertRaises(cfs.BadAuthArguments):
            async with cfs.FhirClient(self.server_url, [], basic_user="User") as server:
                await server.request("GET", "foo")

    async def test_multiple_auth_methods(self):
        """Verify that we fail if given multiple auth args"""
        with self.assertRaises(cfs.BadAuthArguments):
            async with cfs.FhirClient(
                self.server_url, [], basic_user="User", bearer_token="path"
            ) as server:
                await server.request("GET", "foo")

    async def test_get_with_new_header(self):
        """Verify that we can add new headers"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={
                # just to confirm we don't replace default headers entirely
                "Accept": "application/fhir+json",
                "Test": "Value",
            },
        )

        async with cfs.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            # With new header and stream
            await server.request("GET", "foo", headers={"Test": "Value"}, stream=True)

    async def test_get_with_overridden_header(self):
        """Verify that we can overwrite default headers"""
        self.respx_mock.get(
            f"{self.server_url}/bar",
            headers={
                "Accept": "text/plain",  # yay! it's no longer the default fhir+json one
            },
        )

        async with cfs.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            # With overriding a header and default stream (False)
            await server.request("GET", "bar", headers={"Accept": "text/plain"})

    @ddt.data(
        {},  # no keys
        {"keys": [{"key_ops": ["sign"], "kid": "a"}]},  # no alg
        {"keys": [{"alg": "RS384", "kid": "a"}]},  # no key op
        {"keys": [{"alg": "RS384", "key_ops": ["sign"]}]},  # no kid
        {"keys": [{"alg": "RS384", "key_ops": ["verify"], "kid": "a"}]},  # bad key op
    )
    async def test_jwks_without_suitable_key(self, bad_jwks):
        with self.assertRaises(cfs.BadAuthArguments):
            async with cfs.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=bad_jwks
            ):
                pass

    @ddt.data(
        {"token_endpoint": None},
        {"token_endpoint": ""},
    )
    async def test_bad_smart_config(self, bad_config_override):
        """Verify that we require fully correct smart configurations."""
        for entry, value in bad_config_override.items():
            if value is None:
                del self.smart_configuration[entry]
            else:
                self.smart_configuration[entry] = value

        self.respx_mock.reset()
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        with self.assertRaises(cfs.AuthFailed):
            async with cfs.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
            ):
                pass

    async def test_bogus_smart_config(self):
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(content_type="application/json", content=b"invalid json")

        client = cfs.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        )
        with self.assertRaisesRegex(cfs.AuthFailed, "does not expose an OAuth token endpoint"):
            async with client:
                pass

    @ddt.data(
        ({"json": {"error_description": "Ouch!"}}, "Ouch!"),
        (
            {"json": {"error_uri": "http://ouch.com/sadface"}},
            'visit "http://ouch.com/sadface" for more details',
        ),
        # If nothing comes back, we use the default httpx error message
        (
            {},
            (
                "Client error '400 Bad Request' for url 'https://auth.example.com/token'\n"
                "For more information check: "
                "https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400"
            ),
        ),
    )
    @ddt.unpack
    async def test_authorize_error(self, response_params, expected_error):
        """Verify that we translate oauth2 errors into fatal errors."""
        self.respx_mock["token"].respond(400, **response_params)

        with self.assertRaisesRegex(
            cfs.AuthError,
            'An error occurred when connecting to "https://auth.example.com/token": '
            f"\\[400] {expected_error}",
        ):
            async with cfs.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
            ):
                pass

    @ddt.data("bearer_token", "smart_client_id", "basic_password")
    def test_file_read_if_present(self, arg):
        """Verify that if we can't read a provided file, we use it as the secret"""
        with tempfile.NamedTemporaryFile("wt") as file:
            file.write("my-secret\n")
            file.flush()

            with mock.patch("cumulus_fhir_support.client.FhirClient") as mock_init:
                cfs.FhirClient.create_for_cli("/blarg", [], **{arg: "does-not-exist.txt"})
                self.assertEqual(mock_init.call_args[1][arg], "does-not-exist.txt")

                cfs.FhirClient.create_for_cli("/blarg", [], **{arg: file.name})
                self.assertEqual(mock_init.call_args[1][arg], "my-secret")

    async def test_must_be_context_manager(self):
        """Verify that FHIRClient enforces its use as a context manager."""
        client = cfs.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        )
        with self.assertRaisesRegex(RuntimeError, "FhirClient must be used as a context manager"):
            await client.request("GET", "foo")

    @ddt.data(True, False)  # confirm that we handle both stream and non-stream resets
    async def test_get_error_401(self, stream_mode):
        """Verify that an expired token is refreshed."""
        route = self.respx_mock.get(f"{self.server_url}/foo")
        route.side_effect = [httpx.Response(401), httpx.Response(200)]

        async with cfs.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            self.assertEqual(1, self.respx_mock["token"].call_count)

            # Check that we correctly tried to re-authenticate
            response = await server.request("GET", "foo", stream=stream_mode)
            self.assertEqual(200, response.status_code)

            self.assertEqual(2, self.respx_mock["token"].call_count)

    @ddt.data(
        ('{"testing":"hi"}', ".JwKs", {"smart_jwks": {"keys": [{"testing": "hi"}]}}),
        ("hello world", ".PeM", {"smart_pem": "hello world"}),
        ("hello world", ".TxT", cfs.BadAuthArguments),
    )
    @ddt.unpack
    @mock.patch("cumulus_fhir_support.client.FhirClient")
    def test_reads_smart_key(self, contents, suffix, expected_result, mock_client):
        """Verify that we accept smart PEM args"""
        with tempfile.NamedTemporaryFile("wt", suffix=suffix) as file:
            file.write(contents)
            file.flush()
            if isinstance(expected_result, type):
                with self.assertRaises(expected_result):
                    cfs.FhirClient.create_for_cli("/tmp", [], smart_key=file.name)
            else:
                cfs.FhirClient.create_for_cli("/tmp", [], smart_key=file.name)
                for key, val in expected_result.items():
                    self.assertEqual(val, mock_client.call_args[1][key])

    @mock.patch("cumulus_fhir_support.client.FhirClient")
    def test_direct_smart_key(self, mock_client):
        """Verify that we accept a direct JWKS key"""
        cfs.FhirClient.create_for_cli("/tmp", [], smart_key='{"testing": "hi"}')
        self.assertEqual(mock_client.call_args[1]["smart_jwks"], {"keys": [{"testing": "hi"}]})

    @ddt.data(
        (None, 120),  # default to the caller's retry delay
        ("10", 10),  # accept shorter retry delays if the server lets us
        ("200", 120),  # but cap server retry delays by the caller's retry delay
        ("Tue, 14 Sep 2021 21:23:58 GMT", 13),  # parse http-dates too
        ("abc", 120),  # if parsing fails, use caller's retry delay
        ("-5", 0),  # floor of zero
        ("Mon, 13 Sep 2021 21:23:58 GMT", 0),  # floor of zero on dates too
    )
    @ddt.unpack
    @mock.patch("asyncio.sleep")
    async def test_retry_after_parsing(self, retry_after_header, expected_delay, sleep_mock):
        headers = {"Retry-After": retry_after_header} if retry_after_header else {}
        self.respx_mock.get(f"{self.server_url}/file").respond(headers=headers, status_code=503)

        async with cfs.FhirClient(self.server_url, [], bearer_token="foo") as server:
            with self.assertRaises(cfs.TemporaryNetworkError):
                await server.request("GET", "file", retry_delays=[2])
            self.assertEqual(sleep_mock.call_count, 1)
            self.assertEqual(sleep_mock.call_args[0][0], expected_delay)

    @mock.patch("asyncio.sleep")
    async def test_callbacks(self, sleep_mock):
        self.respx_mock.get(f"{self.server_url}/file").respond(status_code=503)
        request_callback = mock.MagicMock()
        error_callback = mock.MagicMock()
        retry_callback = mock.MagicMock()

        async with cfs.FhirClient(self.server_url, [], bearer_token="foo") as server:
            with self.assertRaises(cfs.TemporaryNetworkError):
                await server.request(
                    "GET",
                    "file",
                    retry_delays=[1, 2],
                    request_callback=request_callback,
                    error_callback=error_callback,
                    retry_callback=retry_callback,
                )

        self.assertEqual(sleep_mock.call_count, 2)
        self.assertEqual(sleep_mock.call_args_list[0][0][0], 60)
        self.assertEqual(sleep_mock.call_args_list[1][0][0], 120)

        self.assertEqual(request_callback.call_count, 3)
        self.assertEqual(request_callback.call_args, mock.call())

        self.assertEqual(error_callback.call_count, 3)
        self.assertIsInstance(error_callback.call_args[0][0], cfs.TemporaryNetworkError)

        self.assertEqual(retry_callback.call_count, 2)
        self.assertIsInstance(retry_callback.call_args_list[0][0][0], httpx.Response)
        self.assertEqual(retry_callback.call_args_list[0][0][1], 60)
        self.assertEqual(retry_callback.call_args_list[1][0][1], 120)

    async def test_custom_token_url(self):
        custom_url = "http://example.invalid/custom-token"
        route = self.respx_mock.post(custom_url).respond(json={"access_token": "1234"})

        client = cfs.FhirClient(
            self.server_url,
            [],
            smart_client_id=self.client_id,
            smart_jwks=self.jwks,
            token_url="http://example.invalid/custom-token",
        )
        async with client:
            pass

        # Confirm we didn't call old token
        self.assertEqual(0, self.respx_mock["token"].call_count)
        # But we did call new token
        self.assertEqual(1, route.call_count)

    async def test_full_url_request(self):
        full_url = "https://custom-subdomain.example.com/fhir"
        self.respx_mock.get(full_url).respond(202)

        client = cfs.FhirClient(self.server_url, [])
        async with client:
            response = await client.request("GET", full_url)
            self.assertEqual(response.status_code, 202)

    @ddt.data(
        (None, cfs.FhirClient.MAX_CONNECTIONS),
        (-1, cfs.FhirClient.MAX_CONNECTIONS),
        (0, cfs.FhirClient.MAX_CONNECTIONS),
        (1, 1),
        (2, 2),
    )
    @ddt.unpack
    async def test_custom_max_connections(self, max_in, max_out):
        client = cfs.FhirClient(self.server_url, [], max_connections=max_in)
        self.assertEqual(client._max_connections, max_out)

    async def test_capabilities_prop(self):
        client = cfs.FhirClient(self.server_url, [])
        self.assertEqual(client.capabilities, {})
        async with client:
            self.assertEqual(client.capabilities, {"publisher": "SMART"})

    @ddt.data(
        {"status_code": 400},
        {"status_code": 200, "content": b"bogus json"},
    )
    async def test_metadata_error(self, respond_args):
        self.respx_mock.get(f"{self.server_url}/metadata").respond(**respond_args)

        client = cfs.FhirClient(self.server_url, [])
        async with client:
            # No error, we just silently have no capability statement
            self.assertEqual(client.capabilities, {})


@ddt.ddt
@mock.patch("uuid.uuid4", new=lambda: "1234")
class VendorQuirkTests(unittest.IsolatedAsyncioTestCase):
    """Test case for FHIR client handling of vendor-specific quirks."""

    def setUp(self):
        super().setUp()
        self.server_url = "http://localhost"

        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

    def mock_as_server_type(self, server_type: str | None):
        response_json = {}
        if server_type == "epic":
            response_json = {"software": {"name": "Epic"}}
        elif server_type == "oracle":
            response_json = {"publisher": "Oracle Health"}

        self.respx_mock.get(f"{self.server_url}/metadata").respond(json=response_json)

    @ddt.data(
        ("epic", "present", cfs.ServerType.EPIC),
        ("oracle", "missing", cfs.ServerType.ORACLE),
        (None, "missing", cfs.ServerType.UNKNOWN),
    )
    @ddt.unpack
    async def test_epic_client_id_in_header(self, server_type, expected_text, expected_type):
        # Mock with header
        self.respx_mock.get(
            f"{self.server_url}/file",
            headers={"Epic-Client-ID": "my-id"},
        ).respond(
            text="present",
        )
        # And without
        self.respx_mock.get(
            f"{self.server_url}/file",
        ).respond(
            text="missing",
        )

        self.mock_as_server_type(server_type)
        async with cfs.FhirClient(
            self.server_url, [], bearer_token="foo", smart_client_id="my-id"
        ) as client:
            response = await client.request("GET", "file")
            self.assertEqual(expected_text, response.text)
            self.assertEqual(client.server_type, expected_type)
