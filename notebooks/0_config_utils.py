# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid


# COMMAND ----------

import json
# Read project_config.json and convert it into a Python dictionary.
def load_config():
    config_path = "/Workspace/Users/nivedhajm@gmail.com/CAPSTONE_PROJECT_1/Config/project_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)  #converts JSON into a Python dictionary.
    return config
# why we have load_config() in config_utils? 
# Because many notebooks need it. Instead of copying the same function everywhere, we write it once.

# COMMAND ----------

# MAGIC %md
# MAGIC Generic Reader and Writing Delta Tables

# COMMAND ----------


def read_source(source_config, config):
    path = get_source_path(source_config, config)
    file_format = source_config["format"]
    if file_format == "csv":
        df = (spark.read.option("header", True).option("inferSchema", True).csv(path))
    elif file_format == "json":
        df = spark.read.option("multiLine", True).json(path)
    return df

def write_delta(df, table_name, mode="overwrite"):
    (
        df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
    )

# COMMAND ----------

def add_audit_columns(df):
    return (df.withColumn("_IngestionTimestamp",current_timestamp()))
    
def table_exists(table_name):
    return spark.catalog.tableExists(table_name)

def generate_run_metadata():
    adf_run_id = str(uuid.uuid4())
    batch_id = str(uuid.uuid4())
    return adf_run_id, batch_id

def log_message(msg):
    print(f"{current_timestamp()} : {msg}" )

# COMMAND ----------

# MAGIC %md
# MAGIC Log Message

# COMMAND ----------

def align_to_target_schema(df,
                           target_table,
                           source_name=None,
                           batch_id=None,
                           adf_run_id=None):

    target_schema = spark.table(target_table).schema

    for field in target_schema:

        if field.name not in df.columns:

            if field.name == "_IngestionTimestamp":
                df = df.withColumn(field.name, current_timestamp())

            elif field.name == "_ProcessedTimestamp":
                df = df.withColumn(field.name, current_timestamp())

            elif field.name == "_BatchID":
                df = df.withColumn(field.name, lit(batch_id))

            elif field.name == "_AdfPipelineRunId":
                df = df.withColumn(field.name, lit(adf_run_id))

            elif field.name == "_SourceName":
                df = df.withColumn(field.name, lit(source_name))

            elif field.name == "_IsRejected":
                df = df.withColumn(field.name, lit(False))

            elif field.name == "_RejectReason":
                df = df.withColumn(field.name, lit(None).cast("string"))

            else:
                df = df.withColumn(
                    field.name,
                    lit(None).cast(field.dataType)
                )

    return df.select(*[f.name for f in target_schema])

# COMMAND ----------

def get_source_path(source_config, config, include_file=False):
    storage = config["storage"]
    storage_type = storage["type"]
    if storage_type == "volume":
        base_path = storage["volume_base_path"]
    elif storage_type == "mount":
        base_path = storage["mount_base_path"]
    elif storage_type == "adls":
        base_path = storage["adls_base_path"]
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
    base_path = base_path.rstrip("/")
    if include_file:
        return f"{base_path}/{source_config['source_file']}"
    return f"{base_path}/{source_config['relative_path']}"
    def get_source_config(config, source_name):
    return next(
        source
        for source in config["sources"]
        if source["source_name"] == source_name
    )

def get_last_watermark(table_name, watermark_column):
    if not spark.catalog.tableExists(table_name):
        return None
    value = (spark.table(table_name)
             .agg(max(watermark_column))
             .collect()[0][0]
    )
    return value
def get_last_batch(table_name):
    if not spark.catalog.tableExists(table_name):
        return None
    return (spark.table(table_name)
             .agg(max("_BatchID"))
             .collect()[0][0]
    )