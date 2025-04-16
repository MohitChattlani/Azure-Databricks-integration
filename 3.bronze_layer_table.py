# Databricks notebook source
people_df=spark.read.csv("/mnt/dbfsmohit/people-100.csv",header=True)

# COMMAND ----------

people_df_updated=people_df.withColumnRenamed("Index","index")\
         .withColumnRenamed("User Id","user_id")\
         .withColumnRenamed("First Name","first_name")\
         .withColumnRenamed("Last Name","last_name")\
         .withColumnRenamed("Sex","sex")\
         .withColumnRenamed("Email","email")\
         .withColumnRenamed("Phone","phone")\
         .withColumnRenamed("Date of birth","dob")\
         .withColumnRenamed("Job Title","job_title")

# COMMAND ----------

display(people_df_updated)

# COMMAND ----------

people_df_updated.write.saveAsTable("bronze_db.people")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze_db.people