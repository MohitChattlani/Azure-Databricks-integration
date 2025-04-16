# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<clientid>",
  "fs.azure.account.oauth2.client.secret": "<clientsecret>",
  "fs.azure.account.oauth2.client.endpoint": "<clientendpoint>"
}

# COMMAND ----------

# Example values
storage_account_name = "dbsazuremohit"
container_name = "dbfsmohit"
mount_point = f"/mnt/{container_name}"

# Unmount if already mounted
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mount_point)

# Mount
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls(mount_point))