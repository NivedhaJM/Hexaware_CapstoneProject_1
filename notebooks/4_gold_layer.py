# Databricks notebook source
# MAGIC %run ./0_config_utils

# COMMAND ----------

from datetime import timedelta
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

DIM_DATE_TABLE = "retail.gold.dim_date"
TECHNICAL_COLUMNS = [
    "_AdfPipelineRunId",
    "_IngestionTimestamp",
    "_BatchID",
    "_SourceName",
    "_RejectReason",
    "_IsRejected",
    "_ProcessedTimestamp"
]

def remove_technical_columns(df):
    cols_to_drop = [c for c in TECHNICAL_COLUMNS if c in df.columns]
    return df.drop(*cols_to_drop)

# COMMAND ----------

def generate_dim_date(config):
    orders_config = next(source
        for source in config["sources"]
        if source["source_name"] == "orders"
    )
    orders_df = spark.table(orders_config["silver_table"])
    date_range = (orders_df
        .agg(
            min("OrderDate").alias("min_date"),
            max("OrderDate").alias("max_date")
        )
        .collect()[0]
    )
    start_date = date_range["min_date"]
    end_date = date_range["max_date"]
    dates = []
    current = start_date
    while current <= end_date:
        dates.append((current,))
        current += timedelta(days=1)
    date_df = spark.createDataFrame(dates, ["Date"])
    dim_date = (date_df
        .withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast("int"))
        .withColumn("Year", year("Date"))
        .withColumn("Quarter", quarter("Date"))
        .withColumn("Month", month("Date"))
        .withColumn("MonthName", date_format("Date", "MMMM"))
        .withColumn("WeekOfYear", weekofyear("Date"))
        .withColumn("Day", dayofmonth("Date"))
        .withColumn("DayName", date_format("Date", "EEEE"))
        .withColumn("IsWeekend", dayofweek("Date").isin(1, 7))
    )
    dim_date = dim_date.select(
        "DateKey",
        "Date",
        "Year",
        "Quarter",
        "Month",
        "MonthName",
        "WeekOfYear",
        "Day",
        "DayName",
        "IsWeekend"
    )
    write_delta(dim_date, "retail.gold.dim_date", mode="overwrite")

# COMMAND ----------



def generate_dim_product(source_config):

    print(f"Generating {source_config['gold_table']}")

    # Read Silver table
    product_df = spark.table(source_config["silver_table"])
    product_df = remove_technical_columns(product_df)
    # Generate surrogate key
    primary_key = source_config["primary_key"][0]
    window_spec = Window.orderBy(primary_key)

    product_df = (
        product_df
        .withColumn(
            source_config["surrogate_key"],
            row_number().over(window_spec)
        )
    )

    # Place surrogate key as first column
    product_df = product_df.select(
        source_config["surrogate_key"],
        *[
            c for c in product_df.columns
            if c != source_config["surrogate_key"]
        ]
    )

    # Write Gold table
    (
        product_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(source_config["gold_table"])
    )

    print(f"Successfully created {source_config['gold_table']}")

# COMMAND ----------

def generate_dim_customer(source_config):

    customer_df = spark.table(source_config["scd_table"])
    customer_df = remove_technical_columns(customer_df)
    primary_key = source_config["primary_key"][0]

    window_spec = Window.orderBy(
        primary_key,
        "effective_start_date"
    )

    customer_df = customer_df.withColumn(
        source_config["surrogate_key"],
        row_number().over(window_spec)
    )

    customer_df = customer_df.select(
        source_config["surrogate_key"],
        *[
            c
            for c in customer_df.columns
            if c != source_config["surrogate_key"]
        ]
    )

    (
        customer_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(source_config["gold_table"])
    )

    print(f"Created {source_config['gold_table']}")

# COMMAND ----------

from pyspark.sql.functions import col
def generate_fact_sales(
    orders_config,
    products_config,
    customers_config
):
    print("Generating Fact Sales...")
    # orders_config = next(source
    #     for source in config["sources"]
    #     if source["source_name"] == "orders"
    # )
    # products_config = next(source
    #     for source in config["sources"]
    #     if source["source_name"] == "products"
    # )
    # customers_config = next(source
    #     for source in config["sources"]
    #     if source["source_name"] == "customers"
    # )
    orders_df = spark.table(orders_config["silver_table"])
    dim_product = spark.table(products_config["gold_table"])
    dim_customer = spark.table(customers_config["gold_table"])
    dim_date = spark.table(DIM_DATE_TABLE)
    fact_df = (
        orders_df.alias("o")
        .join(
            dim_product.select(
                "ProductID",
                products_config["surrogate_key"]
            ).alias("p"),
            col("o.ProductID") == col("p.ProductID"),
            "left"
        )
        .select(
            col("o.*"),
            col(f"p.{products_config['surrogate_key']}").alias(products_config["surrogate_key"])
        )
    )
    fact_df = (
        fact_df.alias("f")
        .join(
            dim_date.select(
                "Date",
                "DateKey"
            ).alias("d"),
            col("f.OrderDate") == col("d.Date"),
            "left"
        )
        .select(
            col("f.*"),
            col("d.DateKey")
        )
    )
    fact_df = (
        fact_df.alias("f")
        .join(
            dim_customer.select(
                "CustomerID",
                customers_config["surrogate_key"]
            ).alias("c"),
            col("f.CustomerID") == col("c.CustomerID"),
            "left"
        )
        .select(
            col("f.*"),
            col(f"c.{customers_config['surrogate_key']}").alias(
                customers_config["surrogate_key"]
            )
        )
    )
    fact_df = fact_df.select(
        "OrderID",
        customers_config["surrogate_key"],
        products_config["surrogate_key"],
        "DateKey",
        "Quantity",
        "UnitPrice",
        "StoreCode"
    )

    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(orders_config["gold_table"])
    )

    print(f"Successfully created {orders_config['gold_table']}")

# COMMAND ----------

config = load_config()
generate_dim_date(config)
for source_config in config["sources"]:
    source_name = source_config["source_name"]
    if source_name == "products":
        generate_dim_product(source_config)
    elif source_name == "customers":
        generate_dim_customer(source_config)
orders_config = get_source_config(config, "orders")
products_config = get_source_config(config, "products")
customers_config = get_source_config(config, "customers")
generate_fact_sales(
    orders_config,
    products_config,
    customers_config
)