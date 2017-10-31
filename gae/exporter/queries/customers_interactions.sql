#standardSQL
CREATE TEMP FUNCTION process_sku(sku STRING) AS (
  CASE WHEN (CHAR_LENGTH(sku) - CHAR_LENGTH(REGEXP_REPLACE(sku, r'-', '')) = 3) OR (CHAR_LENGTH(sku) - CHAR_LENGTH(REGEXP_REPLACE(sku, r'-', '')) = 1) THEN REGEXP_EXTRACT(sku, r'(.*)-[0-9A-Z]+')
     ELSE sku END
);


SELECT
  user,
  productSku,
  type
FROM(
  SELECT 
    fullvisitorid user,
    ARRAY(SELECT AS STRUCT process_sku(productSku) productSku, CASE WHEN ecommerceAction.action_type = '2' THEN 1 WHEN ecommerceAction.action_type = '3' THEN 2 ELSE 3 END type FROM UNNEST(hits), UNNEST(product) WHERE ecommerceAction.action_type in ('2', '3', '6')) interactions
  FROM `{project_id}.{dataset_id}.{table_id}`
WHERE TRUE
AND _TABLE_SUFFIX = '{date}'
AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE ecommerceAction.action_type IN ('2', '3', '6'))
),
UNNEST(interactions)

