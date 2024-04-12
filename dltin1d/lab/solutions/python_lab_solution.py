# Databricks notebook source
import dlt

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ## Ingest raw data

# COMMAND ----------

raw_path = "/Volumes/databricks_training/raw/baby_names"

@dlt.table(
  name = "baby_names_bronze",
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)

def baby_names_bronze():          
  return (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load(raw_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumnRenamed("First Name", "first_name") # not allowed for some reason otherwise
  )

# COMMAND ----------

# MAGIC %md ## Transform raw data

# COMMAND ----------

import datetime
current_year = datetime.datetime.now().year

@dlt.table(
  name="baby_names_silver",
  comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "first_name IS NOT NULL")
@dlt.expect_or_fail("valid_count", "count > 0")
@dlt.expect_or_fail("valid_year", f"year <= {current_year}")
def baby_names_silver():
  return (
    dlt.readStream("baby_names_bronze")
      .withColumn("year", F.col("Year").cast("int"))
      .withColumnRenamed("County", "county")
      .withColumnRenamed("Sex", "sex")
      .withColumn("count", F.col("Count").cast("int"))
  )

# COMMAND ----------

# MAGIC %md ## Top baby names

# COMMAND ----------

@dlt.table(
    name = "top_baby_names_2021",
    comment="A table summarizing counts of the top baby names for New York for 2021."
)
def top_baby_names_2021():
  return (
    dlt.read("baby_names_silver")
      .filter(F.col("year") == 2021)
      .groupBy("first_name")
      .agg(F.sum("count").alias("total_count"))
      .sort(F.col("total_count").desc())
      .limit(10)
  )

# COMMAND ----------


