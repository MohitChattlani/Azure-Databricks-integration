# Databricks notebook source
people_df=spark.table("bronze_db.people")

# COMMAND ----------

from pyspark.sql.functions import cast,col

# COMMAND ----------

people_df_transformed=people_df.withColumn("index",col("index").cast("integer"))\
                      .withColumn("dob",col("dob").cast("date"))

# COMMAND ----------

display(people_df_transformed)

# COMMAND ----------

people_df_transformed.write.saveAsTable("silver_db.people")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_db.people;