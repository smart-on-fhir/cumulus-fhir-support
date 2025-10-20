"""HTTP client that talk to a FHIR server"""

import enum
import json
import os
from collections.abc import Callable, Iterable

import httpx

from . import auth, http


class ServerType(enum.Enum):
    UNKNOWN = enum.auto()
    EPIC = enum.auto()
    ORACLE = enum.auto()


class FhirClient:
    """
    Manages authentication and requests for a FHIR server.

    Supports a few different auth methods, but most notably the Backend Service SMART profile.

    Use this as a context manager (like you would an httpx.AsyncClient instance).

    See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
    """

    # Limit the number of connections open at once, because EHRs tend to be very busy.
    MAX_CONNECTIONS = 5

    def __init__(
        self,
        url: str | None,
        resources: Iterable[str],
        *,
        token_url: str | None = None,
        basic_user: str | None = None,
        basic_password: str | None = None,
        bearer_token: str | None = None,
        smart_client_id: str | None = None,
        smart_jwks: dict | None = None,
        smart_pem: str | None = None,
        max_connections: int | None = None,
    ):
        """
        Initialize and authorize a BackendServiceServer context manager.

        :param url: base URL of the SMART FHIR server
        :param resources: a list of FHIR resource names to tightly scope our own permissions
        :param token_url: override for the server-provided OAuth token endpoint
        :param basic_user: username for Basic authentication
        :param basic_password: password for Basic authentication
        :param bearer_token: a bearer token, containing the secret key to sign https requests
        :param smart_client_id: the ID assigned by the FHIR server when registering a new app
        :param smart_jwks: content of a JWK Set file, containing a private key
        :param smart_pem: content of a PEM file, containing a private key
        :param max_connections: override the default limit of simultaneous connections
        """
        self._server_root = url  # all requests are relative to this URL
        if self._server_root and not self._server_root.endswith("/"):
            # This will ensure the last segment does not get chopped off by urljoin
            self._server_root += "/"

        if max_connections is None or max_connections < 1:
            self._max_connections = self.MAX_CONNECTIONS
        else:
            self._max_connections = max_connections

        self._client_id = smart_client_id
        self._server_type = ServerType.UNKNOWN
        self._auth = auth.create_auth(
            self._server_root,
            resources,
            token_url,
            basic_user,
            basic_password,
            bearer_token,
            smart_client_id,
            smart_jwks,
            smart_pem,
        )
        self._session: httpx.AsyncClient | None = None
        self._capabilities: dict = {}

    async def __aenter__(self):
        limits = httpx.Limits(max_connections=self._max_connections)
        timeout = 300  # five minutes to be generous
        # Follow redirects by default -- some EHRs definitely use them for bulk download files,
        # and might use them in other cases, who knows.
        self._session = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
        await self._read_capabilities()  # discover server type, etc
        await self._auth.authorize(self._session)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._session:
            await self._session.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict | None = None,
        stream: bool = False,
        retry_delays: Iterable[int] | None = None,
        request_callback: Callable[[], None] | None = None,
        error_callback: Callable[[http.NetworkError], None] | None = None,
        retry_callback: Callable[[httpx.Response | None, int], None] | None = None,
    ) -> httpx.Response:
        """
        Issues an HTTP request.

        The default Accept type is application/fhir+json, but can be overridden by a provided
        header.

        May raise a NetworkError.

        :param method: HTTP method to issue
        :param path: relative path from the server root to request
        :param headers: optional header dictionary
        :param stream: whether to stream content in or load it all into memory at once
        :param retry_delays: how many minutes to wait between retries, and how many retries to do,
                             defaults to [1, 1] which is three total tries across two minutes.
        :param request_callback: called right before each request
        :param error_callback: called after each network error
        :param retry_callback: called right before sleeping
        :returns: The response object
        """
        if not self._session:
            raise RuntimeError("FhirClient must be used as a context manager")

        url = auth.urljoin(self._server_root, path)

        final_headers = {
            "Accept": "application/fhir+json",
            "Accept-Charset": "UTF-8",
        }
        # merge in user headers with defaults
        final_headers.update(headers or {})

        # Epic wants to see the Epic-Client-ID header, especially for non-OAuth flows.
        # (but I've heard reports of also wanting it in OAuth flows too)
        # See https://fhir.epic.com/Documentation?docId=oauth2&section=NonOauth_Epic-Client-ID-Header
        if self._server_type == ServerType.EPIC and self._client_id:
            final_headers["Epic-Client-ID"] = self._client_id

        final_headers.update(self._auth.sign_headers())

        return await http.http_request(
            self._session,
            method,
            url,
            headers=final_headers,
            stream=stream,
            retry_delays=retry_delays,
            request_callback=request_callback,
            error_callback=error_callback,
            retry_callback=retry_callback,
            auth_callback=self._reauthorize,
        )

    @property
    def server_type(self) -> ServerType:
        """
        Returns the server's type, if detected.
        """
        return self._server_type

    @property
    def capabilities(self) -> dict:
        """
        Returns the server's CapabilityStatement, if available.

        See https://www.hl7.org/fhir/R4/capabilitystatement.html

        If the statement could not be retrieved, this returns an empty dict.
        """
        return self._capabilities

    #############################################################################################
    #
    # Helpers
    #
    #############################################################################################

    async def _read_capabilities(self) -> None:
        """
        Reads the server's CapabilityStatement and sets any properties as a result.

        Notably, this gathers the server/vendor type.
        This is expected to be called extremely early, right as the http session is opened.
        """
        if not self._server_root:
            return

        try:
            response = await http.http_request(
                self._session,
                "GET",
                auth.urljoin(self._server_root, "metadata"),
                headers={
                    "Accept": "application/json",
                    "Accept-Charset": "UTF-8",
                },
            )
        except http.NetworkError:
            return  # That's fine - just skip this optional metadata

        try:
            capabilities = response.json()
        except json.JSONDecodeError:
            return

        if capabilities.get("publisher") in {"Cerner", "Oracle Health"}:
            # Example: https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/metadata?_format=json
            self._server_type = ServerType.ORACLE
        elif capabilities.get("software", {}).get("name") == "Epic":
            # Example: https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/metadata?_format=json
            self._server_type = ServerType.EPIC

        self._capabilities = capabilities

    async def _reauthorize(self) -> dict[str, str]:
        await self._auth.authorize(self._session, reauthorize=True)
        return self._auth.sign_headers()

    #############################################################################################
    #
    # Class constructors
    #
    #############################################################################################

    @classmethod
    def create_for_cli(
        cls,
        fhir_url: str,
        resources: Iterable[str],
        *,
        smart_client_id: str | None = None,
        smart_key: str | None = None,
        basic_password: str | None = None,
        bearer_token: str | None = None,
        **kwargs,
    ) -> "FhirClient":
        """
        Create a FhirClient instance, based on user input from the CLI.

        Since these arguments often are sensitive/secret keys, the user should be encouraged to
        store them in a file and only pass in a filename. So this method reads files from disk to
        get the sensitive tokens, etc.

        But it also allows them to be specified directly, for convenience (either on the user's
        part or the caller may coerce a config file into fake CLI arguments, where allowing
        either a file or a direct secret makes more sense).
        """

        def read_and_strip(path: str) -> str:
            with open(path, encoding="utf8") as f:
                return f.read().strip()

        def read_file_if_present(secret_or_path: str | None) -> str | None:
            if not secret_or_path:
                return None

            # We need to decide if this is a secret or path to a secret.
            # To avoid a syscall with the secret embedded in it, we'll see if it looks like a path
            # and listdir on its parent, to see if a file is present. This still might make a
            # syscall for part of a secret, if the secret has a slash in it. But that's better than
            # it could be.
            folder = os.path.dirname(secret_or_path) or "."
            if os.path.basename(secret_or_path) in os.listdir(folder):
                return read_and_strip(secret_or_path)

            return secret_or_path

        # Try to load client ID from file first (some servers use crazy long ones, like SMART's
        # bulk-data-server, and the user may want to store it as a file)
        smart_client_id = read_file_if_present(smart_client_id)

        basic_password = read_file_if_present(basic_password)
        bearer_token = read_file_if_present(bearer_token)

        # Check deprecated --smart-jwks argument first
        smart_jwks = None
        smart_pem = None
        if smart_key:
            if smart_key.startswith("{"):  # they gave us a key directly
                smart_jwks = json.loads(smart_key)
            else:  # must be a file, let's open it up
                folded = smart_key.casefold()
                if folded.endswith(".jwks") or folded.endswith(".jwk"):
                    smart_jwks = json.loads(read_and_strip(smart_key))
                elif folded.endswith(".pem"):
                    smart_pem = read_and_strip(smart_key)
                else:
                    raise auth.BadAuthArguments(
                        f"Unrecognized private key file '{smart_key}'\n(must end in .jwks or .pem)."
                    )

            # Do we just have a JWK and not a JWK Set? Promote it to a Set.
            if smart_jwks and "keys" not in smart_jwks:
                smart_jwks = {"keys": [smart_jwks]}

        return FhirClient(
            fhir_url,
            resources,
            basic_password=basic_password,
            bearer_token=bearer_token,
            smart_client_id=smart_client_id,
            smart_jwks=smart_jwks,
            smart_pem=smart_pem,
            **kwargs,
        )
