# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type":
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<client-id>",
  "fs.azure.account.oauth2.client.secret":
      dbutils.secrets.get(scope="kvscope", key="client-secret"),
  "fs.azure.account.oauth2.client.endpoint":
      "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
    source="abfss://raw@storageacct.dfs.core.windows.net/",
    mount_point="/mnt/raw",
    extra_configs=configs
)

# COMMAND ----------

# MAGIC %md
# MAGIC to verify

# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

# then update type to mount