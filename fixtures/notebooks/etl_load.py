# Databricks notebook source
# MAGIC %md
# MAGIC # Fixture: load step with streaming (fake code, no real data)

# COMMAND ----------

stream = spark.readStream.table("bronze.events")

# COMMAND ----------

(stream.writeStream
    .option("checkpointLocation", "/Volumes/main/default/chk/events")
    .toTable("silver.events"))

# COMMAND ----------

(stream.writeStream
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/Volumes/main/default/chk/events2")
    .toTable("silver.events2"))
