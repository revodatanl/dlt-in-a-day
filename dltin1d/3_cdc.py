# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW
# MAGIC   users_tempview
# MAGIC AS SELECT
# MAGIC   col1 AS userId,
# MAGIC   col2 AS name,
# MAGIC   col3 AS city,
# MAGIC   col4 AS operation,
# MAGIC   col5 AS sequenceNum
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, "Raul",     "Oaxaca",      "INSERT", 1),
# MAGIC   (123, "Isabel",   "Monterrey",   "INSERT", 1),
# MAGIC   -- New users.
# MAGIC   (125, "Mercedes", "Tijuana",     "INSERT", 2),
# MAGIC   (126, "Lily",     "Cancun",      "INSERT", 2),
# MAGIC   -- Isabel is removed from the system and Mercedes moved to Guadalajara.
# MAGIC   (123, null,       null,          "DELETE", 6),
# MAGIC   (125, "Mercedes", "Guadalajara", "UPDATE", 6),
# MAGIC   -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
# MAGIC   (125, "Mercedes", "Mexicali",    "UPDATE", 5),
# MAGIC   (123, "Isabel",   "Chihuahua",   "UPDATE", 5)
# MAGIC   -- Uncomment to test TRUNCATE.
# MAGIC   -- ,(null, null,      null,          "TRUNCATE", 3)
# MAGIC );

# COMMAND ----------

# MAGIC %md ## SCD type 1

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

@dlt.table
def users_cdc():
  return (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("/Volumes/databricks_training/raw/users_cdc/")
    )

dlt.create_streaming_live_table("target_scd1")
dlt.apply_changes(
  target = "target_scd1",
  source = "users_cdc",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ## SCD type 2

# COMMAND ----------

dlt.create_streaming_live_table("target_scd2")
dlt.apply_changes(
  target = "target_scd2",
  source = "users_cdc",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)
