# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## What are Delta Live Tables expectations?
# MAGIC Expectations are optional clauses you add to Delta Live Tables dataset declarations that apply data quality checks on each record passing through a query.
# MAGIC
# MAGIC **An expectation consists of three things:**
# MAGIC
# MAGIC - A description, which acts as a unique identifier and allows you to track metrics for the constraint.
# MAGIC - A boolean statement that always returns true or false based on some stated condition.
# MAGIC - An action to take when a record fails the expectation, meaning the boolean returns false.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Three basic types
# MAGIC ### Retain invalid record
# MAGIC
# MAGIC `@dlt.expect("valid timestamp", "col(“timestamp”) > '2012-01-01'")`
# MAGIC
# MAGIC ### Drop invalid records
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")
# MAGIC ```
# MAGIC
# MAGIC ### Fail on invalid records
# MAGIC
# MAGIC `@dlt.expect_or_fail("valid_count", "count > 0")`

# COMMAND ----------

# MAGIC %md ## Multiple expectations
# MAGIC
# MAGIC `@dlt.expect_all({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})`
# MAGIC `@dlt.expect_all_or_drop({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})`
# MAGIC `@dlt.expect_all_or_fail({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})`
# MAGIC

# COMMAND ----------

# MAGIC %md ## Quarantine invalid data
# MAGIC - Setup a quarantine table and just direct invalid entries there.
# MAGIC - We will see and example in the aviation use-case.

# COMMAND ----------

# MAGIC %md ## Advanced validations

# COMMAND ----------

# MAGIC %md The following example validates that all expected records are present in the report table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY LIVE TABLE report_compare_tests(
# MAGIC   CONSTRAINT no_missing_records EXPECT (r.key IS NOT NULL)
# MAGIC )
# MAGIC AS SELECT * FROM LIVE.validation_copy v
# MAGIC LEFT OUTER JOIN LIVE.report r ON v.key = r.key
# MAGIC

# COMMAND ----------

# MAGIC %md The following example uses an aggregate to ensure the uniqueness of a primary key:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY LIVE TABLE report_pk_tests(
# MAGIC   CONSTRAINT unique_pk EXPECT (num_entries = 1)
# MAGIC )
# MAGIC AS SELECT pk, count(*) as num_entries
# MAGIC FROM LIVE.report
# MAGIC GROUP BY pk
# MAGIC

# COMMAND ----------

# MAGIC %md ## Make expectations portable and reusable
# MAGIC

# COMMAND ----------

def get_rules_as_list_of_dict():
  return [
    {
      "name": "website_not_null",
      "constraint": "Website IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "location_not_null",
      "constraint": "Location IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "state_not_null",
      "constraint": "State IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "fresh_data",
      "constraint": "to_date(updateTime,'M/d/yyyy h:m:s a') > '2010-01-01'",
      "tag": "maintained"
    },
    {
      "name": "social_media_access",
      "constraint": "NOT(Facebook IS NULL AND Twitter IS NULL AND Youtube IS NULL)",
      "tag": "maintained"
    }
  ]

def get_rules(tag:str):

    all_rules = get_rules_as_list_of_dict()
    selected_rules = [rule for rule in all_rules if rule["tag"]==tag]
    return selected_rules

# COMMAND ----------

rules = get_rules_as_list_of_dict()
get_rules("validity")

# COMMAND ----------


