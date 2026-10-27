# Databricks notebook source
# MAGIC %md
# MAGIC # Fixture: runnable L6 / L13 example (generated data only)
# MAGIC Fails on serverless as written (checkpoint, global temp view, ANSI CAST on bad input).
# MAGIC The tracker's automated rewrite of this notebook runs successfully.

# COMMAND ----------

orders = spark.createDataFrame([(1, "10"), (2, "abc"), (3, "30")], "id INT, amount STRING")
orders = orders.checkpoint()
orders.createOrReplaceGlobalTempView("demo_orders")

# COMMAND ----------

result = spark.sql("""
    SELECT id, CAST(amount AS INT) AS amount_int
    FROM global_temp.demo_orders
""")
rows = result.collect()
print(rows)
assert len(rows) == 3
