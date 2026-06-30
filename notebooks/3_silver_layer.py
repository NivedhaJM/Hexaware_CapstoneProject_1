# Databricks notebook source
# MAGIC %run ./0_config_utils

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

config = load_config()
adf_run_id, batch_id = generate_run_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC GENERAL PROCESS FUNCTION

# COMMAND ----------

CAST_MAP = {
    "orders": {
        "Quantity": "integer",
        "UnitPrice": "double"
    },
    "products": {
        "CostPrice": "double"
    }
}
TRIM_MAP = { 
    "customers": [ "FirstName", "LastName" ]
}

VALIDATION_RULES = {
    "orders": [
        ("Quantity", "not_null"),
        ("Quantity", "positive"),
        ("UnitPrice", "not_null"),
        ("UnitPrice", "positive")
    ],
    "products": [
        ("CostPrice", "not_null"),
        ("CostPrice", "positive"),
        ("ProductName", "not_null")
    ],
    "customers": [
        ("CustomerID", "not_null"),
        ("FirstName", "not_null")
    ],
    "exchange_rates" : [
    ("Rates_ExchangeRate", "positive"),
    ("Rates_TargetCurrency", "not_null"),
    ("BaseCurrency", "not_null"),
    ("RateDate", "not_null")
    ]
}
FILLNA_MAP = {
    "orders": {
        "StoreCode": "UNKNOWN"
    },
    "products":{
        "Brand": "UNKNOWN",
        "Category": "UNKNOWN",
        "SubCategory": "UNKNOWN"
    },
    "customers": {
        "Email": "UNKNOWN",
        "City": "UNKNOWN",
        "State": "UNKNOWN"
    }
}

# COMMAND ----------

def remove_duplicates(df, source_config):
    primary_keys = source_config.get("primary_key")
    if primary_keys is None or len(primary_keys) == 0:
        print(f"Skipping deduplication for {source_config['source_name']}")
        return df
    return df.dropDuplicates(primary_keys)

def cast_columns(df, cast_map):
    for col_name, datatype in cast_map.items():
        df = df.withColumn( col_name, col(col_name).cast(datatype))
    return df

def trim_columns(df, columns):
    for column_name in columns:
        df = df.withColumn(column_name, trim(col(column_name)))
    return df

def validate_source(df, source_name):
    df = (df
        .withColumn("_RejectReason", lit(None))
        .withColumn("_IsRejected", lit(False))
    )
    rules = VALIDATION_RULES[source_name]
    for column_name, rule in rules:
        if rule == "not_null":
            df = (df.withColumn( "_RejectReason",
                when(
                    col(column_name).isNull()
                    & col("_RejectReason").isNull(),
                    f"{column_name} is null"
                ).otherwise(col("_RejectReason"))
            ))
        elif rule == "positive":
            df = (df.withColumn("_RejectReason",
                when(
                    (col(column_name)<=0)
                    & col("_RejectReason").isNull(),
                    f"Invalid {column_name}"
                ).otherwise(col("_RejectReason"))
            ))
    df = df.withColumn( "_IsRejected", col("_RejectReason").isNotNull())
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC SPECIFIC VALIDATION

# COMMAND ----------

def validate_customers(df):
    df = df.withColumn("Email",
        when(~col("Email").rlike( r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' ), "UNKNOWN")
        .otherwise(col("Email"))
    )
    df = df.withColumn("_RejectReason",
        when(
            (~col("Phone").rlike(r'^\d{10}$')) & col("Phone").isNotNull() & col("_RejectReason").isNull(),
            "Invalid Phone"
        ).otherwise(col("_RejectReason"))
    )
    df = df.withColumn("_RejectReason",
        when(
            (col("Email") == "UNKNOWN") & (col("Phone").isNull()) & col("_RejectReason").isNull(),
            "No valid contact information"
        ).otherwise(col("_RejectReason"))
    )
    return df.withColumn("_IsRejected", col("_RejectReason").isNotNull() )

# COMMAND ----------

# MAGIC %md
# MAGIC Flattening JSON

# COMMAND ----------

def flatten_json(df):
    iteration = 0
    max_iterations = 50
    while True:
        iteration += 1
        if iteration > max_iterations:
            raise Exception(f"Maximum flatten depth exceeded ({max_iterations})")
        complex_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType, MapType)):
                complex_cols.append(field.name)
        if not complex_cols:
            break
        field_name = complex_cols[0]
        if field_name not in df.columns:
            continue
        field = df.schema[field_name]
        if isinstance(field.dataType, StructType):
            print(
                f"Iteration {iteration}: "
                f"Flattening Struct Column -> {field_name}"
            )
            expanded_cols = []
            for nested_field in field.dataType.fields:
                expanded_cols.append(
                    col(f"{field_name}.{nested_field.name}")
                    .alias(f"{field_name}_{nested_field.name}")
                )
            remaining_cols = []
            for c in df.columns:
                if c != field_name:
                    remaining_cols.append(c)
            df = df.select(*remaining_cols, *expanded_cols)
        elif isinstance(field.dataType, ArrayType):
            print(
                f"Iteration {iteration}: "
                f"Exploding Array Column -> {field_name}"
            )
            df = df.withColumn(field_name, explode_outer(col(field_name)))
        elif isinstance(field.dataType, MapType):
            print(
                f"Iteration {iteration}: "
                f"MapType Column Found -> {field_name}"
            )
            break
    return df, iteration

# COMMAND ----------

# MAGIC %md
# MAGIC SCD TYPE 2

# COMMAND ----------

def add_scd_columns(df):
    return (df
        .withColumn("effective_start_date", to_timestamp(lit("1900-01-01 00:00:00")))
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

def get_active_records(table_name):
    return (spark.table(table_name).filter(col("is_current") == True))

def build_change_condition(compare_columns):
    condition = None
    for c in compare_columns:
        current_cond = (
            coalesce(col(f"s.{c}"), lit("NULL"))
            !=
            coalesce(col(f"t.{c}"), lit("NULL"))
        )
        condition = (current_cond
            if condition is None
            else condition | current_cond
        )
    return condition

def apply_scd2(source_df, source_config):
    print(source_config)
    table_name = source_config["scd_table"]
    primary_keys = source_config["primary_key"]
    compare_columns = source_config["compare_columns"]
    # First load
    if not spark.catalog.tableExists(table_name):

        initial_df = add_scd_columns(source_df)

        (
            initial_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(f"Initial load completed: {table_name}")
        return
    target_df = get_active_records(table_name)
# Dynamic Join Condition
    join_condition = None
    for pk in primary_keys:
        condition = (source_df[pk] == target_df[pk])
        join_condition = (condition
            if join_condition is None
            else join_condition & condition
        )
# Rename columns after join
    source_cols = [
        col(f"s.{c}").alias(f"s_{c}")
        for c in source_df.columns
    ]
    target_cols = [
        col(f"t.{c}").alias(f"t_{c}")
        for c in target_df.columns
    ]
    joined_df = (source_df.alias("s")
        .join(target_df.alias("t"), join_condition, "left")
        .select(*source_cols, *target_cols)
    )
# Change Detection
    change_condition = None
    change_condition = build_change_condition(compare_columns)
# New Record Detection
    new_condition = None
    for pk in primary_keys:
        condition = col(f"t_{pk}").isNull()
        new_condition = (condition
            if new_condition is None
            else new_condition & condition
        )
    existing_condition = None
    for pk in primary_keys:
        condition = col(f"t_{pk}").isNotNull()
        existing_condition = (condition
            if existing_condition is None
            else existing_condition & condition
        )
    new_records = joined_df.filter(new_condition)
    changed_records = joined_df.filter(existing_condition & change_condition)
    unchanged_records = joined_df.filter(existing_condition & ~change_condition)
    new_count = new_records.count()
    changed_count = changed_records.count()
    print("New Records:", new_count)
    print("Changed Records:", changed_count)
    print("Unchanged Records:", unchanged_records.count())
# Expire old records
    if changed_count > 0:
        changed_keys = changed_records.select(
            *[col(f"s_{pk}").alias(pk)
                for pk in primary_keys]
        )
        delta_table = DeltaTable.forName(spark, table_name )
        merge_condition = " AND ".join([f"t.{pk}=s.{pk}" for pk in primary_keys] )
        (
            delta_table.alias("t")
            .merge(changed_keys.alias("s"), merge_condition)
            .whenMatchedUpdate(
                condition="t.is_current = true",
                set={
                    "is_current": "false",
                    "effective_end_date": "current_timestamp()"
                }
            ).execute()
        )
# New Versions
    changed_insert_df = (
        changed_records.select(
            *[
                col(f"s_{c}").alias(c)
                for c in source_df.columns]
        )
    )
# Brand New Records
    new_insert_df = (new_records.select(
            *[
                col(f"s_{c}").alias(c)
                for c in source_df.columns
            ]
        )
    )
    dfs_to_insert = []
# Changed Records
    if changed_count > 0:
        changed_insert_df = add_scd_columns(changed_insert_df)
        changed_insert_df = align_to_target_schema(
            changed_insert_df, table_name,
            source_name=source_config["source_name"],
            batch_id=batch_id,
            adf_run_id=adf_run_id
        )
        dfs_to_insert.append(changed_insert_df)
# New Records
    if new_count > 0:
        new_insert_df = add_scd_columns(new_insert_df)
        new_insert_df = align_to_target_schema(
            new_insert_df, table_name,
            source_name=source_config["source_name"],
            batch_id=batch_id,
            adf_run_id=adf_run_id
        )
        dfs_to_insert.append(new_insert_df)
# Final Insert
    if dfs_to_insert:
        final_insert_df = dfs_to_insert[0]
        for df in dfs_to_insert[1:]:
            final_insert_df = final_insert_df.unionByName(df)
        (
            final_insert_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )
        print(f"Inserted {final_insert_df.count()} rows.")
    else:
        print("No new or changed records to insert.")
print("SCD2 processing completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC SCHEMA EVOLUTION

# COMMAND ----------

def evolve_schema(source_df, table_name):
    if not spark.catalog.tableExists(table_name):
        print(f"{table_name} does not exist. Skipping schema evolution.")
        return
    target_schema = spark.table(table_name).schema
    target_columns = {
        field.name: field.dataType.simpleString()
        for field in target_schema
    }
    new_columns = []
    for field in source_df.schema.fields:
        if field.name not in target_columns:
            new_columns.append(f"{field.name} {field.dataType.simpleString()}")
    if not new_columns:
        print(f"No schema changes detected for {table_name}")
        return
    cols_sql = ", ".join(new_columns)
    spark.sql(f"""
        ALTER TABLE {table_name}
        ADD COLUMNS ({cols_sql})
    """)
    print(f"Added columns to {table_name}: {', '.join(new_columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC DRIVER LOOP

# COMMAND ----------

audit_metrics = []
for source_config in config["sources"]:
    print(source_config["source_name"])
    print(source_config.keys())
    source_name = source_config["source_name"]
    bronze_df = spark.table(source_config["bronze_table"])
    evolve_schema(bronze_df, source_config["silver_table"])
    if source_config["scd_type"] == "type2":
        evolve_schema(bronze_df, source_config["scd_table"])
    df = remove_duplicates(bronze_df, source_config)
    if source_name in CAST_MAP:
        df = cast_columns(df,CAST_MAP[source_name])
    if source_name in TRIM_MAP:
        df = trim_columns(df, TRIM_MAP[source_name])
    if source_name in FILLNA_MAP:
        df = df.fillna(FILLNA_MAP[source_name])
    if source_name == "exchange_rates":
        df, flatten_steps = flatten_json(df)
    df = validate_source(df, source_name)
    if source_name == "customers":
        df = validate_customers(df)
    valid_df = (df.filter(~col("_IsRejected"))
          .withColumn("_ProcessedTimestamp",current_timestamp())
    )
    rejected_df = (df.filter(col("_IsRejected"))
          .withColumn("_ProcessedTimestamp",current_timestamp())
    )
    total_count = df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    audit_metrics.append({
        "SourceName": source_name,
        "TotalRecords": total_count,
        "ValidRecords": valid_count,
        "RejectedRecords": rejected_count,
        "ProcessedTimestamp": datetime.now()
    })
    if source_config["scd_type"] == "type2":
        apply_scd2(valid_df, source_config)
    else:
        write_delta(valid_df, source_config["silver_table"])
    write_delta(rejected_df, f"retail.audit.rejected_{source_name}")
audit_df = spark.createDataFrame(audit_metrics)
write_delta(audit_df, "retail.audit.load_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC SANITY CHECK

# COMMAND ----------

write_delta(audit_df, "retail.audit.load_summary")
print("SILVER LAYER SANITY CHECK")
display(audit_df)

# COMMAND ----------

customers_df = spark.table("retail.silver.customers")

customers_scd2_test = add_scd_columns(customers_df)

customers_scd2_test.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.silver.customers_scd2_test")

# COMMAND ----------

# case 1 
# customer_scd_config = {
#     "source_name": "customers",
#     "silver_table": "retail.silver.customers_scd2_test",
#     "primary_key": ["CustomerID"],
#     "compare_columns": [
#         "FirstName",
#         "LastName",
#         "Email",
#         "Phone",
#         "City",
#         "State"
#     ]
# }
# source_df = spark.table("retail.silver.customers")

# apply_scd2(
#     source_df,
#     customer_scd_config
# )

# COMMAND ----------

# case 2
# new_customer = (
#     spark.table("retail.silver.customers")
#     .limit(1)
#     .withColumn("CustomerID", lit(9999))
#     .withColumn("FirstName", lit("Test"))
#     .withColumn("LastName", lit("User"))
#     .withColumn("City", lit("Chennai"))
#     .withColumn("State", lit("Tamil Nadu"))
# )

# test_df = (
#     spark.table("retail.silver.customers")
#     .unionByName(new_customer)
# )

# apply_scd2(
#     test_df,
#     customer_scd_config
# )

# COMMAND ----------

# case 3
# test_df = (
#     spark.table("retail.silver.customers_scd2_test")
#     .withColumn(
#         "City",
#         when(
#             col("CustomerID") == 9999,
#             "Bangalore"
#         ).otherwise(col("City"))
#     )
# )

# apply_scd2(
#     test_df,
#     customer_scd_config
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail.silver.customers_scd2_test

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # VERIFICATION

# COMMAND ----------

bronze_df = spark.table("retail.bronze.customers")
display(bronze_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.silver.customers 

# COMMAND ----------

display(spark.table("retail.audit.rejected_customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC