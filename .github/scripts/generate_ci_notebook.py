"""
CI notebook deployer.

Reads the bundled ci-interactive-notebook.ipynb template, injects the Parameters
cell with runtime values, patches the lakehouse metadata, and uploads to the
ephemeral Fabric workspace via the Items API.

Replaces inject_notebook.py (VD-1880). No NOTEBOOK_GLOB — the notebook is
owned by domain-cicd, not the domain repo.

Environment variables required:
  EPHEMERAL_WORKSPACE_ID, EPHEMERAL_WORKSPACE_NAME
  EPHEMERAL_LAKEHOUSE_ID, EPHEMERAL_LAKEHOUSE_NAME (optional, defaults to vdephelh)
  HEAD_SHA
  PROJECT_ZIP_ABFSS  — AC-32: ABFSS URI of the dbt project zip uploaded to OneLake Files
  PROD_STATE_ABFSS (optional, falls back to ./prod-state)
  CI_TARGET (optional, defaults to ephemeral_ci)
  CI_RUN_ID (optional, defaults to "")

Note: dbt-fabricspark (Microsoft-maintained) is installed at notebook runtime via !pip install.
Fabric Environment pip packages do not apply to Jupyter Python kernels, so the
environment is used only to configure the workspace-default Spark pool.
"""

import base64
import copy
import json
import os
from pathlib import Path

import fabric_transport
import runner_io

_BUNDLE_INTERACTIVE = Path(__file__).parent.parent / "notebooks" / "ci-interactive-notebook.ipynb"


def build_parameters_cell_source(
    *,
    workspace_id: str,
    workspace_name: str,
    lakehouse_id: str,
    lakehouse_name: str,
    head_sha: str,
    project_zip_abfss: str,
    ci_run_id: str,
    ci_target: str,
    prod_state_path: str,
    prod_workspace_name: str = "",
    prod_lakehouse_name: str = "",
    gate: str = "2",
    schema_name: str = "dbo",
) -> list[str]:
    """Build the Parameters cell source lines from already-fetched values.

    AC-32: emits project_zip_abfss (OneLake zip path); no GitHub App / vault fields.
    Returns scalar string assignments only — no f-string commands.
    The template's command-assembly cell builds dbt command strings at
    notebook runtime using local_prod_state_path, which is set by the prod-state cell.
    """
    return [
        "# Parameters — injected by CI (do not edit manually)\n",
        f'ci_target = "{ci_target}"\n',
        f'prod_state_path = "{prod_state_path}"\n',
        f'project_zip_abfss = "{project_zip_abfss}"\n',
        f'lakehouse_name = "{lakehouse_name}"\n',
        f'lakehouse_id = "{lakehouse_id}"\n',
        f'workspace_id = "{workspace_id}"\n',
        f'workspace_name = "{workspace_name}"\n',
        f'schema_name = "{schema_name}"\n',
        f'gate = "{gate}"\n',
        f'ci_run_id = "{ci_run_id}"\n',
        f'head_sha = "{head_sha}"\n',
        f'prod_workspace_name = "{prod_workspace_name}"\n',
        f'prod_lakehouse_name = "{prod_lakehouse_name}"\n',
    ]


def inject_parameters_cell(notebook: dict, source: list[str]) -> dict:
    """Replace the Parameters cell in the template with the provided source lines.

    Identifies the Parameters cell by the text 'Parameters' in its source.
    Returns a deep copy — input is not mutated.
    """
    nb = copy.deepcopy(notebook)
    for cell in nb.get("cells", []):
        src = cell.get("source", [])
        if isinstance(src, str):
            src = src.splitlines(keepends=True)
        if cell.get("cell_type") == "code" and any("Parameters" in line for line in src):
            cell["source"] = source
            return nb
    # If no Parameters cell found, insert at position 0
    print("Warning: No Parameters cell found in template. Inserting at position 0.", flush=True)
    nb["cells"].insert(0, {
        "cell_type": "code",
        "source": source,
        "metadata": {"tags": ["parameters"]},
        "outputs": [],
        "execution_count": None,
    })
    return nb


def ipynb_to_fabric_py(notebook: dict) -> str:
    """Convert a Jupyter notebook dict to Fabric's Python notebook format.

    Fabric Items API requires path='notebook-content.py' with content in this
    format — standard Jupyter JSON with path='notebook-content.ipynb' is rejected.

    Fabric expects the entire metadata JSON pretty-printed with '# META ' prefix
    on every line (not one '# META {key}' line per key). Each cell also needs a
    trailing '# METADATA' block with language/language_group for Fabric to honour
    the Python kernel instead of defaulting to synapse_pyspark.
    """
    def _meta_block(obj: dict) -> str:
        """Serialize a dict as a # META -prefixed pretty-printed block."""
        lines = []
        for line in json.dumps(obj, indent=2).splitlines():
            lines.append(f"# META {line}\n")
        return "".join(lines)

    result = ["# Fabric notebook source\n"]

    metadata = notebook.get("metadata", {})
    if metadata:
        result.append("\n# METADATA ********************\n\n")
        result.append(_meta_block(metadata))

    for cell in notebook.get("cells", []):
        cell_type = cell.get("cell_type", "code")
        cell_meta = cell.get("metadata", {})
        tags = cell_meta.get("tags", [])
        source = "".join(cell.get("source", []))

        if cell_type == "code":
            if "parameters" in tags:
                result.append("\n# PARAMETERS CELL ********************\n\n")
            else:
                result.append("\n# CELL ********************\n\n")
            result.append(source)
            if source and not source.endswith("\n"):
                result.append("\n")
            # Per-cell metadata — Fabric uses this to set the kernel per cell.
            # Without it Fabric defaults every cell to synapse_pyspark.
            ms = cell_meta.get("microsoft", {})
            cell_lang_meta = {
                "language": ms.get("language", "python"),
                "language_group": ms.get("language_group", "jupyter_python"),
            }
            result.append("\n# METADATA ********************\n\n")
            result.append(_meta_block(cell_lang_meta))
        elif cell_type == "markdown":
            result.append("\n# MARKDOWN CELL ********************\n\n")
            for md_line in source.splitlines(keepends=True):
                result.append(f"# {md_line}" if md_line.strip() else "#\n")

    return "".join(result)


def patch_lakehouse_metadata(notebook: dict, lakehouse_id: str, lakehouse_name: str, workspace_id: str) -> dict:
    """Patch metadata.dependencies.lakehouse to the ephemeral lakehouse.

    Fabric restores the default-lakehouse context from # META lines in the
    serialized notebook. Without this patch, notebookutils.fs calls resolve to
    the prod lakehouse, not the ephemeral one.
    Returns a deep copy — input is not mutated.
    """
    nb = copy.deepcopy(notebook)
    nb.setdefault("metadata", {}).setdefault("dependencies", {})["lakehouse"] = {
        "default_lakehouse": lakehouse_id,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": workspace_id,
        "known_lakehouses": [{"id": lakehouse_id, "name": lakehouse_name, "workspaceId": workspace_id}],
    }
    return nb


def find_existing_notebook(workspace_id: str, display_name: str) -> str | None:
    """Return item ID of an existing notebook with the given display name, or None."""
    resp = fabric_transport.request("GET", f"/workspaces/{workspace_id}/items")
    for item in resp.get("value", []):
        if item["type"] == "Notebook" and item["displayName"] == display_name:
            return item["id"]
    return None


def upload_notebook(workspace_id: str, display_name: str, notebook: dict) -> str | None:
    """Create or update a notebook in the Fabric workspace via Items API.

    Returns the notebook item ID. Callers are responsible for emitting
    notebook_id to GITHUB_OUTPUT when needed.
    """
    nb_content = base64.b64encode(ipynb_to_fabric_py(notebook).encode()).decode()
    definition = {
        "parts": [{
            "path": "notebook-content.py",
            "payload": nb_content,
            "payloadType": "InlineBase64",
        }]
    }

    existing_id = find_existing_notebook(workspace_id, display_name)
    if existing_id:
        print(f"Updating existing notebook: {display_name} ({existing_id})", flush=True)
        fabric_transport.request_long_running(
            "POST",
            f"/workspaces/{workspace_id}/items/{existing_id}/updateDefinition",
            {"definition": definition},
        )
        notebook_id = existing_id
    else:
        print(f"Creating notebook: {display_name}", flush=True)
        body = fabric_transport.request_long_running(
            "POST",
            f"/workspaces/{workspace_id}/items",
            {"displayName": display_name, "type": "Notebook", "definition": definition},
        )
        notebook_id = body.get("id") or find_existing_notebook(workspace_id, display_name)

    print(f"Notebook '{display_name}' is now available in the workspace.", flush=True)
    if notebook_id:
        print(f"Notebook ID: {notebook_id}", flush=True)

    return notebook_id


def main(interactive_path: Path | None = None) -> None:
    workspace_id = os.environ["EPHEMERAL_WORKSPACE_ID"]
    workspace_name = os.environ["EPHEMERAL_WORKSPACE_NAME"]
    lakehouse_id = os.environ["EPHEMERAL_LAKEHOUSE_ID"]
    lakehouse_name = os.environ.get("EPHEMERAL_LAKEHOUSE_NAME", "vdephelh")
    head_sha = os.environ["HEAD_SHA"].strip()
    if not head_sha:
        raise ValueError("HEAD_SHA environment variable is empty — cannot build session ID file path")
    project_zip_abfss = os.environ.get("PROJECT_ZIP_ABFSS", "").strip()
    if not project_zip_abfss:
        raise ValueError("PROJECT_ZIP_ABFSS is empty — upload-dbt-project step must set abfss_path before notebook deploy")
    ci_run_id = os.environ.get("CI_RUN_ID", "").strip()
    ci_target = os.environ.get("CI_TARGET", "").strip() or "ephemeral_ci"

    prod_state_abfss = os.environ.get("PROD_STATE_ABFSS", "").strip() or "./prod-state"
    if prod_state_abfss.endswith("/manifest.json"):
        prod_state_abfss = prod_state_abfss[: -len("/manifest.json")]
    prod_workspace_name = os.environ.get("PROD_WORKSPACE_NAME", "").strip()
    prod_lakehouse_name = os.environ.get("PROD_LAKEHOUSE_NAME", "").strip()

    params_source = build_parameters_cell_source(
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        lakehouse_id=lakehouse_id,
        lakehouse_name=lakehouse_name,
        head_sha=head_sha,
        project_zip_abfss=project_zip_abfss,
        ci_run_id=ci_run_id,
        ci_target=ci_target,
        prod_state_path=prod_state_abfss,
        prod_workspace_name=prod_workspace_name,
        prod_lakehouse_name=prod_lakehouse_name,
    )

    if interactive_path is None:
        interactive_path = _BUNDLE_INTERACTIVE
    with open(interactive_path) as f:
        interactive = json.load(f)
    interactive = inject_parameters_cell(interactive, params_source)
    interactive = patch_lakehouse_metadata(interactive, lakehouse_id, lakehouse_name, workspace_id)
    interactive_id = upload_notebook(workspace_id, "ci-interactive-notebook", interactive)
    if interactive_id:
        runner_io.set_output("interactive_notebook_id", interactive_id)


if __name__ == "__main__":
    main()
