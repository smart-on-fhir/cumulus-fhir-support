"""FHIR support code for the Cumulus project"""

__version__ = "1!0.0.0"

from .auth import AuthError, AuthFailed, BadAuthArguments
from .client import FhirClient, ServerType
from .errors import RequestError
from .http import (
    FatalNetworkError,
    NetworkError,
    TemporaryNetworkError,
    http_request,
    parse_retry_after,
)
from .ml_json import (
    list_multiline_json_in_dir,
    read_multiline_json,
    read_multiline_json_from_dir,
    read_multiline_json_with_details,
)
from .notes import (
    NoteFilter,
    RefScanner,
    RefSet,
    RefsNotFound,
    RemoteAttachment,
    anon_id,
    anon_ref,
    get_text_from_note_res,
    make_note_filter,
    make_note_ref_scanner,
    note_res_has_text,
)
from .schemas import pyarrow_schema_from_rows
