#standardSQL
SELECT
  user,
  productSku,
  type
FROM(
  SELECT 
    fullvisitorid user,
    ARRAY(SELECT AS STRUCT productSku, CASE WHEN ecommerceAction.action_type = '2' THEN 1 WHEN ecommerceAction.action_type = '3' THEN 2 ELSE 3 END type FROM UNNEST(hits), UNNEST(product) WHERE ecommerceAction.action_type in ('2', '3', '6')) interactions
  FROM `{project_id}.{dataset_id}.{table_id}`
WHERE TRUE
AND _TABLE_SUFFIX = '{date}'
AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE ecommerceAction.action_type IN ('2', '3', '6'))
LIMIT 1000
),
UNNEST(interactions)

