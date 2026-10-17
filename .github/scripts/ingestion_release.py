"""Deliver a tracked code folder to the flat OneLake folder its notebook reads (VD-5847).

The dlt runner copies `/lakehouse/default/Files/<slug>/ingestion` at run time and a dbt
job notebook `Files/<slug>/transformation`, so each folder is the one copy: git is the
version store. The folder is deleted before the upload, because OneLake keeps whatever
was written there and code the repo has since removed would otherwise stay importable. `revision.txt` goes last, so a
delivery that did not finish leaves none — a marker for whoever inspects the folder;
the runner does not check it. Every byte is validated (`plan_delivery`) before anything
is deleted, so a delivery that cannot be made leaves the working one in place. The
replace itself is not atomic: a write failing after the delete leaves the folder
partly written until the next delivery. The deploy delivers before applying any item,
so a first deploy's schedule never exists without its code.

Mirrors the plugin's `upload-to-onelake.py` (the sandbox delivery) as a parallel copy;
nothing serialises two deliveries to one folder beyond the deploy workflow's
per-branch concurrency group.
"""
import os
import re
import stat

try:
    from scripts import fabric_transport
except ImportError:
    import fabric_transport

FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 256 * 1024 * 1024
FILE_COUNT_LIMIT = 10000
_COMPONENT = re.compile(r"[A-Za-z0-9_.-]{1,255}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


def safe_path(path):
    if not isinstance(path, str) or len(path) > 1024:
        raise ValueError("invalid source path")
    parts = path.split("/")
    if len(parts) > 32 or any(p in (".", "..") or not _COMPONENT.fullmatch(p) for p in parts):
        raise ValueError("unsafe source path")
    return path


def safe_component(value):
    safe_path(value)
    if "/" in value:
        raise ValueError("expected one path component")
    return value


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
            raise ValueError("source must be a regular file")
        with os.fdopen(os.dup(fd), "rb") as stream:
            data = stream.read(FILE_LIMIT + 1)
        if len(data) > FILE_LIMIT:
            raise ValueError("source file exceeds bounds")
        return data
    except FileNotFoundError:
        return None  # tracked deletion: absent from this delivered revision
    except OSError as exc:
        raise ValueError("source cannot be opened safely") from exc
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
            raise ValueError("prohibited source file")


def safe_blob_key(path):
    """Full Blob name budget, separate from the repository-relative path budget."""
    # Azure HNS allows 63 segments including the account and workspace/container.
    if not isinstance(path, str) or not 1 <= len(path) <= 1024 or len(path.split("/")) > 61:
        raise ValueError("physical Blob key exceeds provider limits")
    for part in path.split("/"):
        safe_component(part)
    return path


def delivery_root(lakehouse_id, slug, folder="ingestion"):
    """`<lakehouse>/Files/<slug>/<folder>` — the folder the notebook's attachment mounts."""
    return safe_blob_key(f"{safe_component(lakehouse_id)}/Files/{safe_component(slug)}/{safe_component(folder)}")


def create_blob(workspace_id, path, data):
    # Create-only: the folder was just emptied, so an existing blob means a concurrent writer.
    fabric_transport.blob_request("PUT", workspace_id, safe_blob_key(path), data=data,
                                  headers={"x-ms-blob-type": "BlockBlob", "If-None-Match": "*"})


def plan_delivery(root, files, commit, folder="ingestion"):
    """Every destination blob for `files` (keyed `<folder>/<rel>`), validated; raises ValueError."""
    if not 1 <= len(files) <= FILE_COUNT_LIMIT or sum(map(len, files.values())) > TOTAL_LIMIT:
        raise ValueError(f"{folder} snapshot exceeds bounds or is empty")
    if not isinstance(commit, str) or not _COMMIT.fullmatch(commit):
        raise ValueError(f"{folder} delivery needs the deployed commit sha")
    targets = {}
    for path, data in sorted(files.items()):
        if not path.startswith(f"{folder}/"):
            raise ValueError(f"invalid {folder} source path")
        rel = safe_path(path[len(folder) + 1:])
        if rel == "revision.txt":
            raise ValueError("revision.txt is written by the deployer, never committed")
        targets[safe_blob_key(f"{root}/{rel}")] = data
    return targets


def deliver(workspace_id, root, files, commit, folder="ingestion"):
    """Replace the folder with exactly `files`, revision.txt last; returns files written."""
    targets = plan_delivery(root, files, commit, folder)
    fabric_transport.dfs_delete_recursive(workspace_id, safe_blob_key(root))
    for target, data in targets.items():
        create_blob(workspace_id, target, data)
    create_blob(workspace_id, f"{root}/revision.txt", (commit + "\n").encode())
    return len(targets)
