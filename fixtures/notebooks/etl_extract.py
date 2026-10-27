# Databricks notebook source
# MAGIC %md
# MAGIC # Fixture: extract step (fake code, no real data)

# COMMAND ----------

df = spark.table("raw.orders").filter("amount > 0")
df = df.cache()  # supported on serverless: must NOT be flagged
df.write.mode("overwrite").saveAsTable("curated.orders_extract")
