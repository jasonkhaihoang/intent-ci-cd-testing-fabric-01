# Design: Seed Salesforce account source and build staging model

## Architecture

- **Grain:** one row per Salesforce account (`account_id`).
- **Materialization:** `stg_salesforce__accounts` is a view — the staging layer
  is already configured as `view` in `dbt_project.yml`.
- **Approach:** seed the raw `account` dataset with `dbt seed`, register it as
  the dbt source `salesforce.account` in `models/sources.yml`, then build a 1:1
  staging model that renames Salesforce column names to snake_case and drops
  soft-deleted rows (`is_deleted = FALSE`).
- **Decision:** source name `salesforce` and model name `stg_salesforce__accounts`
  follow the `stg_{source}__{entity}` convention. No business logic in staging —
  joins and CASE expressions are deferred to intermediate/mart layers.

## Inventory

### Model Inventory

| Model | Layer | Grain | Source |
| --- | --- | --- | --- |
| `stg_salesforce__accounts` | staging | one row per account_id | `source('salesforce', 'account')` |

The raw dataset is the committed seed `transformation/seeds/account.csv`
(materialized as the `account` table), registered as the source
`salesforce.account`.

## Source Mapping / Discovery

`dbt seed` -> `account` (lakehouse `dbo`) -> `source('salesforce', 'account')`
-> `stg_salesforce__accounts` (view).

## Change Impact

Fresh build: `transformation/models/` was empty before this change, so no
existing models are touched and there are no downstream consumers to impact.
The staging model adds no new schema and changes no existing contract.

## Approvals

Requested as a direct, minimal execution — no design-reviewer was dispatched and
the design stop was not raised for this self-contained change.
