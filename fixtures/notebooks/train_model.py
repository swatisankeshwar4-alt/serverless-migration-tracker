# Databricks notebook source
model_df = spark.table("ml.features")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("scala cell inside a python notebook")
