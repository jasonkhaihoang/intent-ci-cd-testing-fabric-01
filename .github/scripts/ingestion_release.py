"""Create-only ingestion staging and conditional, single-Blob-PUT publication."""
import concurrent.futures
import os
import re
import stat
import xml.etree.ElementTree as ET

try:
    from scripts import fabric_transport
except ImportError:
    import fabric_transport

from ingestion_manifest import (
    ENTRY_LIMIT, FILE_COUNT_LIMIT, FILE_LIMIT, POINTER_LIMIT, TOTAL_LIMIT,
    canonical_json, parse_manifest, parse_pointer, safe_component, safe_path, sha256,
)


def read_source(source, rel):
    """Open each component without following links, including a replaced parent."""
    safe_path(rel)
    descriptors = []
    try:
        fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(fd)
        parts = rel.split("/")
        for part in parts[:-1]:
            fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            descriptors.append(fd)
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        descriptors.append(fd)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("ingestion source must be a regular file")
        with os.fdopen(os.dup(fd), "rb") as stream:
            data = stream.read(FILE_LIMIT + 1)
        if len(data) > FILE_LIMIT:
            raise ValueError("ingestion source file exceeds bounds")
        return data
    except FileNotFoundError:
        return None  # tracked deletion: absent from this delivered revision
    except OSError as exc:
        raise ValueError("ingestion source cannot be opened safely") from exc
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def check_source_path(rel):
    """Local delivery guard, not a replacement for domain-cicd's full publish policy."""
    safe_path(rel)
    parts = rel.split("/")
    forbidden = {".git", ".vibedata", "__pycache__", ".agents_tmp", ".pytest_cache", ".ruff_cache",
                 ".mypy_cache", "node_modules", "dbt_packages", ".venv", "venv", "target", "logs"}
    secrets = {"secrets.toml", ".user.yml", "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"}
    for part in parts:
        if part in forbidden or part in secrets or part == ".env" or (
            part.startswith(".env.") and part not in {
                ".env.example", ".env.sample", ".env.template", ".env.dist", ".env.defaults"
            }
        ) or part.endswith((".pyc", ".log", ".duckdb", ".duckdb.wal")):
            raise ValueError("prohibited ingestion source file")


def build_manifest(files):
    if not 1 <= len(files) <= FILE_COUNT_LIMIT or sum(map(len, files.values())) > TOTAL_LIMIT:
        raise ValueError("ingestion snapshot exceeds bounds or is empty")
    data = canonical_json({"schemaVersion": 1, "files": [
        {"path": p, "size": len(b), "sha256": sha256(b)} for p, b in sorted(files.items())
    ]})
    revision = sha256(data)
    parse_manifest(data, revision)
    return data, revision


def require_etag(etag):
    # Accept one opaque quoted tag or a legacy bare token, never a wildcard/list.
    # Missing metadata must not select creation or remove replacement conditions.
    if not isinstance(etag, str) or not re.fullmatch(r'"[\x21\x23-\x7e]+"|[A-Za-z0-9._:-]+', etag):
        raise ValueError("OneLake requires one nonempty ETag")
    return etag


def safe_blob_key(path):
    """Full Blob name budget, separate from the manifest-relative wire budget."""
    # Azure HNS allows 63 segments including the account and workspace/container.
    if not isinstance(path, str) or not 1 <= len(path) <= 1024 or len(path.split("/")) > 61:
        raise ValueError("physical Blob key exceeds provider limits")
    for part in path.split("/"):
        safe_component(part)
    return path


def preflight_release(root, files, revision):
    """Validate every physical destination before any release read or mutation."""
    safe_blob_key(root)
    parts = root.split("/")
    if len(parts) != 4 or parts[1] != "Files" or parts[3] != ".vibedata":
        raise ValueError("invalid ingestion release namespace")
    prefix = f"{root}/ingestion-releases/{revision}"
    safe_blob_key(root + "/ingestion-current.json")
    for relative in [*files, "manifest.json"]:
        safe_blob_key(f"{prefix}/{safe_path(relative)}")
    return prefix


class BlobReleaseStore:
    """Bounded Blob operations through the bundle's existing storage-token transport."""
    def __init__(self, workspace):
        self.workspace = safe_component(workspace)

    def read(self, path, limit):
        safe_blob_key(path)
        data, headers = fabric_transport.blob_request("GET", self.workspace, path, limit=limit)
        etag = require_etag(headers.get("ETag"))
        if len(data) != int(headers.get("Content-Length", "-1")):
            raise ValueError("release blob was truncated or size unavailable")
        return data, etag

    def put(self, path, data, *, etag=None):
        safe_blob_key(path)
        conditions = {"If-None-Match": "*"} if etag is None else {"If-Match": require_etag(etag)}
        fabric_transport.blob_request("PUT", self.workspace, path, data=data,
                                      headers={"x-ms-blob-type": "BlockBlob", **conditions}, limit=4096)

    def inventory(self, prefix, limit):
        safe_blob_key(prefix)
        found, directories, seen, markers = {}, set(), set(), set()
        marker = None
        for _ in range(1000):
            params = {"restype": "container", "comp": "list", "prefix": prefix + "/",
                      "include": "metadata", "maxresults": "1000"}
            if marker:
                params["marker"] = marker
            data, _ = fabric_transport.blob_request("GET", self.workspace, "", params=params,
                                                    limit=2 * 1024 * 1024)
            document = ET.fromstring(data)
            if document.tag != "EnumerationResults" or document.find("Blobs") is None:
                raise ValueError("malformed release inventory")
            for blob in document.findall("Blobs/Blob"):
                name = blob.findtext("Name") or ""
                if not name.startswith(prefix + "/"):
                    raise ValueError("release inventory outside requested prefix")
                safe_blob_key(name)
                rel = safe_path(name[len(prefix) + 1:])
                if rel in seen or len(seen) >= limit:
                    raise ValueError("release inventory duplicate or limit exceeded")
                seen.add(rel)
                if any(blob.findtext("Metadata/" + flag) == "true" for flag in ("hdi_isfolder", "is_directory")):
                    directories.add(rel)
                    continue
                size = int(blob.findtext("Properties/Content-Length") or "-1")
                if not 0 <= size <= FILE_LIMIT:
                    raise ValueError("invalid release inventory size")
                found[rel] = size
            marker = document.findtext("NextMarker")
            if not marker:
                ancestors = {"/".join(p.split("/")[:i]) for p in found for i in range(1, len(p.split("/")))}
                if directories - ancestors:
                    raise ValueError("release contains extra empty directory")
                return found
            if marker in markers:
                raise ValueError("release inventory repeated marker")
            markers.add(marker)
        raise ValueError("release inventory page limit exceeded")


def stage_release(store, root, files, source_commit):
    """Stage/verify create-only bytes without changing the active pointer."""
    manifest, revision = build_manifest(files)
    desired = canonical_json({"schemaVersion": 1, "revision": revision, "sourceCommit": source_commit})
    parse_pointer(desired)
    prefix = preflight_release(root, files, revision)
    pointer_path = root + "/ingestion-current.json"
    try:
        prior, etag = store.read(pointer_path, POINTER_LIMIT)
        require_etag(etag)
        parse_pointer(prior)
    except FileNotFoundError:
        prior, etag = None, None
    staged = {**files, "manifest.json": manifest}
    for path, data in sorted(staged.items()):
        target = f"{prefix}/{path}"
        try:
            store.put(target, data)
        except FileExistsError:
            if store.read(target, len(data))[0] != data:
                raise ValueError("immutable release path contains different bytes") from None
    if store.inventory(prefix, ENTRY_LIMIT) != {p: len(b) for p, b in staged.items()}:
        raise ValueError("immutable release has missing or extra files")
    for path, data in staged.items():
        if store.read(f"{prefix}/{path}", len(data))[0] != data:
            raise ValueError("immutable release verification failed")
    return {"path": pointer_path, "prior": prior, "etag": etag, "desired": desired, "revision": revision}


def commit_release(store, staged):
    """Publish last; lost responses are read back, never retried as unconditional writes."""
    path, prior, desired = staged["path"], staged["prior"], staged["desired"]
    # Even equal bytes need CAS: another deployment may publish during item applies.
    try:
        store.put(path, desired, etag=staged["etag"])
    except (concurrent.futures.CancelledError, fabric_transport.ConditionalWriteUnsupported):
        raise
    except Exception as commit_error:
        for _ in range(3):
            try:
                actual = store.read(path, POINTER_LIMIT)[0]
            except FileNotFoundError:
                actual = None
            except concurrent.futures.CancelledError:
                raise
            except Exception:
                continue
            if actual == desired:
                return staged["revision"]
            if actual == prior:
                raise RuntimeError("ingestion pointer not committed; prior pointer observed") from commit_error
            raise RuntimeError("ingestion commit outcome unknown; another pointer observed") from commit_error
        raise RuntimeError("ingestion commit outcome unknown; pointer readback unavailable") from commit_error
    return staged["revision"]
