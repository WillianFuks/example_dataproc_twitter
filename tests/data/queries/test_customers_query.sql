#standardSQL
SELECT
  table_id,
  dataset_id,
  project_id,
  date_
FROM(
  SELECT
    '{table_id}' AS table_id,
    '{dataset_id}' AS dataset_id,
    '{project_id}' AS project_id,
    '{date}' AS date_
  )
