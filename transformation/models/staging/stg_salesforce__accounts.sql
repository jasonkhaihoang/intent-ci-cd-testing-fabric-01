-- Model: stg_salesforce__accounts
-- Grain: one row per Salesforce account.
-- Source: source('salesforce', 'account')

SELECT
  id AS account_id,
  name AS account_name,
  type AS account_type,
  industry,
  billingcity AS billing_city,
  billingstate AS billing_state,
  billingcountry AS billing_country,
  ownerid AS owner_id,
  createddate AS created_date,
  lastmodifieddate AS last_modified_date
FROM {{ source('salesforce', 'account') }}
WHERE isdeleted = FALSE
