# Databricks notebook source
silver_df=spark.table("silver_db.people")

# COMMAND ----------

jdbcHostname = "<dbname>.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "<dbname>"

jdbcUsername = "jdbcuser"
jdbcPassword = "jdbcPassword"  #Use dbutils.secrets.get() in prod

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"


# COMMAND ----------
# writing into Azure dbo.users sql server table
silver_df.write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("dbtable", "dbo.users") \
  .option("user", jdbcUsername) \
  .option("password", jdbcPassword) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .mode("append") \
  .save()