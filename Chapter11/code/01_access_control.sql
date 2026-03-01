-- Create a security schema for all masking and filter functions
CREATE SCHEMA IF NOT EXISTS cat_dev.sandbox;

-- Masking function: email addresses
CREATE OR REPLACE FUNCTION cat_dev.sandbox.mask_email(email_value STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('pii_viewers') THEN email_value
  ELSE CONCAT(LEFT(email_value, 2), '****@', SPLIT(email_value, '@')[1])
END;

-- Masking function: phone numbers (show last 4 digits)
CREATE OR REPLACE FUNCTION cat_dev.sandbox.mask_phone(phone_value STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('pii_viewers') THEN phone_value
  ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone_value, '[^0-9]', ''), 4))
END;

-- Masking function: monetary values (show ranges for non-finance)
CREATE OR REPLACE FUNCTION cat_dev.sandbox.mask_revenue(revenue DECIMAL(10,2))
RETURNS STRING
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('finance_team') THEN CAST(revenue AS STRING)
  WHEN revenue > 10000 THEN 'High (>10K)'
  WHEN revenue > 1000 THEN 'Medium (1K-10K)'
  ELSE 'Low (<1K)'
END;

-- Row filter: regional access control
CREATE OR REPLACE FUNCTION cat_dev.sandbox.filter_by_region(region_value STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('global_admins') THEN TRUE
  WHEN IS_ACCOUNT_GROUP_MEMBER('emea_team') 
    AND region_value IN ('EU', 'UK', 'EMEA') THEN TRUE
  WHEN IS_ACCOUNT_GROUP_MEMBER('na_team') 
    AND region_value IN ('US', 'CA') THEN TRUE
  WHEN IS_ACCOUNT_GROUP_MEMBER('apac_team') 
    AND region_value IN ('APAC', 'JP', 'AU') THEN TRUE
  ELSE FALSE
END;

-- Apply column masks
ALTER TABLE cat_dev.sandbox.customer_analytics_gold
ALTER COLUMN email SET MASK cat_dev.sandbox.mask_email;

ALTER TABLE cat_dev.sandbox.customer_analytics_gold
ALTER COLUMN phone SET MASK cat_dev.sandbox.mask_phone;

ALTER TABLE cat_dev.sandbox.customer_analytics_gold
ALTER COLUMN total_revenue SET MASK cat_dev.sandbox.mask_revenue;

-- Apply row filter
ALTER TABLE cat_dev.sandbox.customer_analytics_gold
SET ROW FILTER cat_dev.sandbox.filter_by_region ON (region);

-- Tag columns for data discovery
ALTER TABLE cat_dev.sandbox.customer_analytics_gold
  ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
ALTER TABLE cat_dev.sandbox.customer_analytics_gold
  ALTER COLUMN phone SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
ALTER TABLE cat_dev.sandbox.customer_analytics_gold
  SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true');
