-- Databricks notebook source
-- MAGIC %md ## Ingest raw data

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE baby_names_bronze_sql COMMENT "Popular baby first names in New York. This data was ingested from the New York State Departement of Health." AS
SELECT
  `Year`,
  Count,
  Sex,
  County,
  current_timestamp() as ingestion_timestamp,
  `FIRST NAME` AS first_name
FROM
  cloud_files(
    "/Volumes/databricks_training/raw/baby_names",
    "csv"
  )

-- COMMAND ----------

-- MAGIC %md ## Transform raw data

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE baby_names_silver_sql(
  CONSTRAINT valid_first_name EXPECT (first_name IS NOT NULL),
  CONSTRAINT valid_count EXPECT (count > 0) ON VIOLATION FAIL
  UPDATE,
    CONSTRAINT valid_year EXPECT (`year` <= 2024) ON VIOLATION FAIL
  UPDATE
) COMMENT "New York popular baby first name data cleaned and prepared for analysis." AS
SELECT
  CAST(`Year` AS INT) AS year,
  CAST(Count AS INT) as count,
  Sex as sex,
  County as county,
  first_name
FROM
  STREAM(live.baby_names_bronze_sql);

-- COMMAND ----------

-- MAGIC %md ## Top baby names

-- COMMAND ----------

CREATE
OR REFRESH MATERIALIZED VIEW top_baby_names_2021_sql COMMENT "A table summarizing counts of the top baby names for New York for 2021." AS
SELECT
  first_name,
  SUM(count) AS total_count
FROM
  (live.baby_names_silver_sql)
WHERE
  year = 2021
GROUP BY
  first_name
ORDER BY
  total_count DESC
LIMIT
  10;
