# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run "./0_environment_setup"

# COMMAND ----------

NOTEBOOKS = [
    "0_environment_setup",
    "0_config_utils",
    "1_data_generator",
    "2_bronze_layer",
    "3_silver_layer",
    "4_gold_layer"
]
# Environment setup
print("Running 0_environment_setup...")
dbutils.notebook.run("0_environment_setup", 0)

for notebook in NOTEBOOKS:
    try:
        print(f"Running {notebook}...")
        dbutils.notebook.run(notebook, 0)
        print(f"{notebook} completed successfully.\n")

    except Exception as e:
        print(f"{notebook} failed.")
        raise e
print("Retail Lakehouse Pipeline Completed Successfully")