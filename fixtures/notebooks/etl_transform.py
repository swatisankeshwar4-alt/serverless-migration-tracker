# Databricks notebook source
# MAGIC %md
# MAGIC # Fixture: transform step (fake code, no real data)

# COMMAND ----------

orders = spark.table("raw.orders")
orders = orders.cache()

# COMMAND ----------

clean = orders.filter("amount IS NOT NULL")
clean = clean.checkpoint()
clean.createOrReplaceGlobalTempView("clean_orders")

# COMMAND ----------

result = spark.sql("""
    SELECT order_id,
           CAST(amount AS INT) AS amount_int,
           total / item_count AS avg_item
    FROM global_temp.clean_orders
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE hot_orders AS SELECT * FROM raw.orders WHERE status = 'open'
