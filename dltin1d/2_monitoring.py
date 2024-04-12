# Databricks notebook source
# MAGIC %md ## Query event tables

# COMMAND ----------

pipeline_id = "9955a7fe-4f1b-4042-a911-2481a82c3e62"

event_df = spark.sql(f'SELECT * FROM event_log("{pipeline_id}")')
display(event_df)

# COMMAND ----------

table_name = "databricks_training.aviation.gold_aviation"

event_df = spark.sql(f'SELECT * FROM event_log(TABLE({table_name}))')
display(event_df)

# COMMAND ----------

event_df.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMP VIEW latest_update AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   event_log_raw
# MAGIC WHERE
# MAGIC   event_type = 'create_update'
# MAGIC ORDER BY
# MAGIC   timestamp DESC
# MAGIC LIMIT
# MAGIC   1;

# COMMAND ----------

# MAGIC %sql select * from latest_update

# COMMAND ----------

# MAGIC %md ## Query lineage information from the event log
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   details :flow_definition.output_dataset as output_dataset,
# MAGIC   details :flow_definition.input_datasets as input_dataset
# MAGIC FROM
# MAGIC   event_log_raw,
# MAGIC   latest_update
# MAGIC WHERE
# MAGIC   event_type = 'flow_definition'
# MAGIC   AND origin.update_id = latest_update.id

# COMMAND ----------

# MAGIC %md ## Query data quality from the event log
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw,
# MAGIC       latest_update
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id = latest_update.id
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name
# MAGIC

# COMMAND ----------

# MAGIC %md ## Monitor Enhanced Autoscaling events from the event log

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'RESIZING' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as starting_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'SUCCEEDED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as succeeded_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'PARTIALLY_SUCCEEDED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as partially_succeeded_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'FAILED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as failed_num_executors
# MAGIC FROM
# MAGIC   event_log_raw,
# MAGIC   latest_update
# MAGIC WHERE
# MAGIC   event_type = 'autoscale'
# MAGIC   AND
# MAGIC   origin.update_id = latest_update.id

# COMMAND ----------

# MAGIC %md ## Monitor compute resource utilization

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   Double(details :cluster_resources.avg_task_slot_utilization) as utilization
# MAGIC FROM
# MAGIC   event_log_raw,
# MAGIC   latest_update
# MAGIC WHERE
# MAGIC   event_type = 'cluster_resources'
# MAGIC   AND
# MAGIC   origin.update_id = latest_update.id

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   Double(details :cluster_resources.num_executors) as current_executors,
# MAGIC   Double(details :cluster_resources.latest_requested_num_executors) as latest_requested_num_executors,
# MAGIC   Double(details :cluster_resources.optimal_num_executors) as optimal_num_executors,
# MAGIC   details :cluster_resources.state as autoscaling_state
# MAGIC FROM
# MAGIC   event_log_raw,
# MAGIC   latest_update
# MAGIC WHERE
# MAGIC   event_type = 'cluster_resources'
# MAGIC   AND
# MAGIC   origin.update_id = latest_update.id

# COMMAND ----------

# MAGIC %md ## User actions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details :user_action :action,
# MAGIC   details :user_action :user_name
# MAGIC FROM
# MAGIC   event_log_raw
# MAGIC WHERE
# MAGIC   event_type = 'user_action'

# COMMAND ----------

# MAGIC %md ## Runtime information

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   details :create_update :runtime_version :dbr_version
# MAGIC FROM
# MAGIC   event_log_raw
# MAGIC WHERE
# MAGIC   event_type = 'create_update'

# COMMAND ----------


