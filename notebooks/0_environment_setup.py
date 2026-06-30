# Databricks notebook source
# %run ./0_mount_adls

# Mounting is infrastructure setup. It should happen once.

# COMMAND ----------

dbutils.widgets.dropdown( "environment", "free_edition", ["free_edition", "community", "azure"], "Environment" )  # not much necessary
dbutils.widgets.text( "catalog_name", "retail", "Catalog Name" )
dbutils.widgets.text( "schemas", "bronze,silver,gold,audit", "Schemas" )
dbutils.widgets.text( "volumes", "raw_files,processed_files", "Volumes" )

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")
schemas = dbutils.widgets.get("schemas").split(",")
volumes = dbutils.widgets.get("volumes").split(",")

# COMMAND ----------

spark.sql(f""" CREATE CATALOG IF NOT EXISTS {catalog_name} """)

for schema in schemas:
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} """)

for volume in volumes:
    spark.sql(f""" CREATE VOLUME IF NOT EXISTS {catalog_name}.bronze.{volume} """)

spark.sql(f""" CREATE TABLE IF NOT EXISTS {catalog_name}.audit.pipeline_log (
run_id STRING,
table_name STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
rows_read BIGINT,
rows_written BIGINT,
status STRING,
error_message STRING
) USING DELTA
""")
# watermark_metadata is used for incremental loading
spark.sql(f""" CREATE TABLE IF NOT EXISTS {catalog_name}.audit.watermark_metadata (
table_name STRING,
watermark_value TIMESTAMP
) USING DELTA
""")