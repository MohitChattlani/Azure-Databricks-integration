-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bronze_db
LOCATION '/mnt/dbfsmohit/bronze_db';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver_db
LOCATION '/mnt/dbfsmohit/silver_db';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold_db
LOCATION '/mnt/dbfsmohit/gold_db';