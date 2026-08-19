---
kinds: [transformation]
---

# Intent: Seed Salesforce account source and build staging model

## Goal
Establish a minimal end-to-end dbt path for a raw Salesforce account dataset:
land the raw data, register it as a dbt source, and produce a staging model
that renames columns and removes soft-deleted rows. Proves the source -> staging
boundary works before any business logic is added.

## Source system
Salesforce — raw `account` records seeded into the lakehouse.

## Target
Microsoft Fabric ephemeral lakehouse (`dbo` schema), dbt `fabric_domain` project.

## Objects in scope
- Salesforce `account` object (raw).

## Deliverables inventory

| # | Deliverable | Kind | Notes |
| --- | --- | --- | --- |
| 1 | Raw account dataset seeded + registered as `salesforce.account` source | mart/model | `transformation/seeds/account.csv` + `transformation/models/sources.yml`. |
| 2 | `stg_salesforce__accounts` staging model | mart/model | 1:1 view; renames columns, filters soft-deletes. |

## Success criteria
- `dbt seed` materializes the raw `account` table.
- `dbt run` builds `stg_salesforce__accounts` as a view.
- `account_id` is not-null and unique in the staging model.

## Out of scope
- No intermediate or mart models.
- No business logic (CASE expressions, joins).
- No orchestration or semantic-model artifacts.
- No ingestion via dlt and no external source connection.

## Open questions
- None.

## Approvals

Requested as a direct, minimal execution — no separate intent-approval gate was
raised for this self-contained seed + staging change.
