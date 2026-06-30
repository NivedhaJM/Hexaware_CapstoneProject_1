# Databricks notebook source
# MAGIC %run ./0_config_utils
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

config = load_config()

# Create Pipeline Run ID
# (Temporary placeholder until ADF integration)
adf_run_id, batch_id = generate_run_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC INGESTION

# COMMAND ----------

def ingestion():
    overall_status = "SUCCESS"
    for source_config in config["sources"]:
        try:
            source_name = source_config["source_name"]
            bronze_table = source_config["bronze_table"]
            print(f"Processing {source_name}")
            df = read_source(source_config, config)
            load_type = source_config.get("load_type", "full")
            mode = "overwrite"
            if load_type == "incremental":
                inc_type = source_config.get("incremental_type")
                if inc_type == "watermark":
                    watermark_col = source_config["watermark_column"]
                    last_watermark = get_last_watermark(bronze_table, watermark_col)
                    if last_watermark is not None:
                        df = df.filter(col(watermark_col) > last_watermark)
                elif inc_type == "batch":
                    if spark.catalog.tableExists(bronze_table):
                        existing_keys = (spark.table(bronze_table)
                                .select(source_config["primary_key"])
                                .distinct()
                        )
                        df = (df.join( existing_keys,
                                on=source_config["primary_key"],
                                how="left_anti"
                            )
                        )
                mode = "append"
            rows_read = df.count()
            if rows_read == 0:
                print(f"{source_name}: No new records to load")
                continue
            df = (df
                .withColumn("_AdfPipelineRunId", lit(adf_run_id))
                .withColumn("_IngestionTimestamp", current_timestamp())
                .withColumn("_BatchID", lit(batch_id))
                .withColumn("_SourceName", lit(source_name))
            )
            write_delta(df, bronze_table, mode)
            print(f"{source_name}: {rows_read} rows loaded ({mode})")
        except Exception as e:
            overall_status = "FAILED"
            print(f"Error processing {source_name}: {e}")
    return overall_status

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity Check

# COMMAND ----------

def bronze_sanity_check():
    for source_config in config["sources"]:
        table_name = source_config["bronze_table"]
        primary_key = source_config["primary_key"]
        df = spark.table(table_name)
        total_rows = df.count()
        if primary_key:
            duplicate_count = (total_rows - df.dropDuplicates(primary_key).count())
        else:
            duplicate_count = None
        print(f"""
        Table : {table_name}
        Total rows : {total_rows}
        Duplicate rows : {duplicate_count}
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC Run

# COMMAND ----------

status = ingestion()
if status == "SUCCESS":
    bronze_sanity_check()
else:
    print("Bronze sanity check skipped because ingestion failed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from retail.bronze.customers