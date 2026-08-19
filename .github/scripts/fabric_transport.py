"""Unified Fabric/Azure transport: token acquisition, retry, LRO polling, DFS writes.

Public surface:
  get_token(audience) -> str
  request(method, path, body=None, audience='fabric', retries=3) -> dict
  request_long_running(method, path, body, audience='fabric', timeout_s=120, poll_interval_s=5) -> dict
  dfs_request(method, url, audience='storage', data=None, params=None) -> None

Audiences: fabric, powerbi, storage.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

def _fabric_api_base() -> str:
    """Read FABRIC_API_BASE_URL per call (not at import time) so a fake-world
    test can override it without reloading this module — same pattern as
    emit_status.py's GITHUB_API_BASE_URL / fetch_prod_state.py's
    ONELAKE_DFS_BASE_URL."""
    return os.environ.get("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com/v1")


def _powerbi_api_base() -> str:
    """Read POWERBI_API_BASE_URL per call — same pattern as _fabric_api_base()."""
    return os.environ.get("POWERBI_API_BASE_URL", "https://api.powerbi.com/v1.0/myorg")


FABRIC_API = _fabric_api_base()  # kept for existing external readers; reflects env at import time only
POWERBI_API = _powerbi_api_base()  # kept for existing external readers; reflects env at import time only

_AUDIENCE_RESOURCE: dict[str, str] = {
    "fabric": "https://api.fabric.microsoft.com",
    "powerbi": "https://analysis.windows.net/powerbi/api",
    "storage": "https://storage.azure.com",
}


def _base_url(audience: str) -> str:
    if audience == "fabric":
        return _fabric_api_base()
    if audience == "powerbi":
        return _powerbi_api_base()
    raise KeyError(audience)


def get_token(audience: str) -> str:
    """Acquire an Azure access token for the given audience via az CLI."""
    resource = _AUDIENCE_RESOURCE[audience]
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", resource],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)["accessToken"]


def _retry_delay(code: int, attempt: int, headers) -> int:
    """Return seconds to wait before retry attempt. 430 uses exponential backoff; others honor Retry-After."""
    if code == 430:
        return 30 * (2 ** attempt)
    if code == 500:
        return 1
    return int(headers.get("Retry-After", 5))


def request(method: str, path: str, body: dict = None, audience: str = "fabric", retries: int = 3) -> dict:
    """Fabric REST API call with retry on 429/430/500/503 honoring Retry-After."""
    base = _base_url(audience)
    url = f"{base}{path}"
    data = json.dumps(body).encode() if body else None

    for attempt in range(retries):
        token = get_token(audience)
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code in (429, 430, 500, 503) and attempt < retries - 1:
                delay = _retry_delay(e.code, attempt, e.headers)
                print(f"Retrying after {delay}s (attempt {attempt + 1}/{retries}): {e.code} {e.msg}", flush=True)
                time.sleep(delay)
                continue
            body_text = e.read().decode(errors="replace")
            print(f"HTTP {e.code} {method} {url}: {body_text}", file=sys.stderr)
            raise
    raise RuntimeError(f"Failed after {retries} retries: {method} {path}")


def request_multipart(method: str, path: str, file_content: bytes, filename: str, content_type: str = "text/plain", audience: str = "fabric", retries: int = 3) -> dict:
    """Fabric REST API call with a multipart/form-data file payload.

    Used for endpoints that require file upload (e.g. staging/libraries).
    """
    boundary = b"boundary12345"
    body = (
        b"--" + boundary + b"\r\n"
        b'Content-Disposition: form-data; name="file"; filename="' + filename.encode() + b'"\r\n'
        b"Content-Type: " + content_type.encode() + b"\r\n"
        b"\r\n"
        + file_content +
        b"\r\n--" + boundary + b"--\r\n"
    )

    base = _base_url(audience)
    url = f"{base}{path}"

    for attempt in range(retries):
        token = get_token(audience)
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary.decode()}")
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code in (429, 430) and attempt < retries - 1:
                delay = _retry_delay(e.code, attempt, e.headers)
                print(f"Retrying after {delay}s (attempt {attempt + 1}/{retries}): {e.code} {e.msg}", flush=True)
                time.sleep(delay)
                continue
            body_text = e.read().decode(errors="replace")
            print(f"HTTP {e.code} {method} {url}: {body_text}", file=sys.stderr)
            raise
    raise RuntimeError(f"Failed after {retries} retries: {method} {path}")


def request_long_running(
    method: str, path: str, body: dict,
    audience: str = "fabric", timeout_s: int = 120, poll_interval_s: int = 5,
) -> dict:
    """POST/PATCH/PUT to Fabric; handle 202 + Location/operationId polling.

    Returns the initial response body dict (contains item id on 201; empty on 202).
    Raises RuntimeError on LRO failure or timeout.
    """
    base = _base_url(audience)
    url = f"{base}{path}"
    token = get_token(audience)
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.status
            location = resp.getheader("Location")
            raw = resp.read()
            parsed = (json.loads(raw) or {}) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        print(f"HTTP {e.code} {method} {url}: {body_text}", file=sys.stderr)
        raise

    if not location and "operationId" in parsed:
        location = f"{_fabric_api_base()}/operations/{parsed['operationId']}"

    if status == 202:
        _poll_operation(location, token, timeout_s, poll_interval_s)

    return parsed


def _poll_operation(
    operation_url: str | None, token: str, timeout_s: int, poll_interval_s: int
) -> None:
    if not operation_url:
        raise RuntimeError(
            "Fabric returned 202 Accepted but no operation URL was found in the "
            "Location header or response body."
        )
    print(f"Polling Fabric operation: {operation_url}", flush=True)
    deadline = time.monotonic() + timeout_s
    last_status = "Unknown"
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        req = urllib.request.Request(operation_url, method="GET")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body_text = e.read().decode(errors="replace")
            raise RuntimeError(
                f"Failed to poll Fabric operation (HTTP {e.code}): {body_text}"
            ) from e
        last_status = result.get("status", "Unknown")
        print(f"  Operation status [{attempt}]: {last_status}", flush=True)
        if last_status == "Succeeded":
            return
        if last_status == "Failed":
            error = result.get("error", {})
            msg = f"{error.get('errorCode', 'UnknownError')}: {error.get('message', str(result))}"
            raise RuntimeError(f"Fabric operation failed — {msg}")
        time.sleep(poll_interval_s)
    raise RuntimeError(
        f"Fabric operation timed out after {timeout_s}s "
        f"(last status: {last_status!r}). "
        "The notebook may not be available in the workspace."
    )


def dfs_request(
    method: str, url: str, audience: str = "storage", data: bytes = None, params: dict = None
) -> None:
    """ADLS Gen2 DFS REST call (PUT/PATCH). Raises on non-2xx."""
    token = get_token(audience)
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    effective_data = data if data is not None else b""
    req = urllib.request.Request(url, data=effective_data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Length", str(len(effective_data)))
    try:
        with urllib.request.urlopen(req):
            pass
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        print(f"HTTP {e.code} {method} {url}: {body_text}", file=sys.stderr)
        raise
