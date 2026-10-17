"""Wire parity with vd-data-engineering ingestion_manifest.py at 74dc0157.

Keep validation and canonical serialization aligned with the reviewed runner.
"""
import hashlib
import json
import re
from concurrent.futures import CancelledError

POINTER_LIMIT = 4096
MANIFEST_LIMIT = 2 * 1024 * 1024
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 256 * 1024 * 1024
FILE_COUNT_LIMIT = 10000
ENTRY_LIMIT = 20000
_HEX256 = re.compile(r"[0-9a-f]{64}\Z")
_COMPONENT = re.compile(r"[A-Za-z0-9_.-]{1,255}\Z")


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(path):
    if not isinstance(path, str) or len(path) > 1024:
        raise ValueError("invalid ingestion path")
    parts = path.split("/")
    if len(parts) > 32 or any(p in (".", "..") or not _COMPONENT.fullmatch(p) for p in parts):
        raise ValueError("unsafe ingestion path")
    return path


def safe_component(value):
    safe_path(value)
    if "/" in value:
        raise ValueError("expected one path component")
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _json(data, limit):
    if not data or len(data) > limit:
        raise ValueError("ingestion metadata size exceeds bounds")
    obj = json.loads(data, object_pairs_hook=_unique_object)
    if not isinstance(obj, dict) or type(obj.get("schemaVersion")) is not int or obj["schemaVersion"] != 1:
        raise ValueError("unsupported ingestion schemaVersion")
    return obj


def parse_pointer(data):
    obj = _json(data, POINTER_LIMIT)
    if set(obj) not in ({"schemaVersion", "revision"}, {"schemaVersion", "revision", "sourceCommit"}):
        raise ValueError("invalid ingestion pointer fields")
    if not isinstance(obj["revision"], str) or not _HEX256.fullmatch(obj["revision"]):
        raise ValueError("invalid ingestion revision")
    if "sourceCommit" in obj and (
        not isinstance(obj["sourceCommit"], str)
        or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", obj["sourceCommit"])
    ):
        raise ValueError("invalid ingestion sourceCommit")
    return obj


def parse_manifest(data, revision):
    if sha256(data) != revision:
        raise ValueError("ingestion manifest digest mismatch")
    obj = _json(data, MANIFEST_LIMIT)
    if set(obj) != {"schemaVersion", "files"} or not isinstance(obj["files"], list):
        raise ValueError("invalid ingestion manifest fields")
    if not 1 <= len(obj["files"]) <= FILE_COUNT_LIMIT:
        raise ValueError("ingestion file count exceeds bounds")
    files, total = {}, 0
    for item in obj["files"]:
        if not isinstance(item, dict) or set(item) != {"path", "size", "sha256"}:
            raise ValueError("invalid ingestion inventory entry")
        path = safe_path(item["path"])
        if not path.startswith("ingestion/") or path in files:
            raise ValueError("invalid or duplicate ingestion inventory path")
        size = item["size"]
        if type(size) is not int or not 0 <= size <= FILE_LIMIT:
            raise ValueError("ingestion file size exceeds bounds")
        if not isinstance(item["sha256"], str) or not _HEX256.fullmatch(item["sha256"]):
            raise ValueError("invalid ingestion file digest")
        total += size
        if total > TOTAL_LIMIT:
            raise ValueError("ingestion total bytes exceeds bounds")
        files[path] = item
    if list(files) != sorted(files) or canonical_json(obj) != data:
        raise ValueError("ingestion manifest is not canonical")
    # A file cannot also be another file's ancestor.
    for path in files:
        parts = path.split("/")
        if any("/".join(parts[:i]) in files for i in range(1, len(parts))):
            raise ValueError("ingestion path collides with a directory")
    return files


def confirmed_absence(exc):
    # Never infer absence from a message, exists(False), or an authorization failure.
    return isinstance(exc, FileNotFoundError) or (
        getattr(exc, "status_code", None) == 404
        and getattr(exc, "error_code", None) in ("BlobNotFound", "PathNotFound", "ResourceNotFound")
    )


def provider_call(operation, *args, **kwargs):
    """Authenticated provider bodies are not safe notebook terminal output."""
    try:
        return operation(*args, **kwargs)
    except CancelledError:
        raise
    except FileExistsError:
        raise FileExistsError("conditional ingestion write conflict") from None
    except Exception as exc:
        if confirmed_absence(exc):
            raise FileNotFoundError("ingestion provider path not found") from None
        status = getattr(exc, "status_code", None)
        status = status if type(status) is int else "unknown"
        raise RuntimeError(
            f"ingestion provider operation failed ({type(exc).__name__}, HTTP {status})"
        ) from None
