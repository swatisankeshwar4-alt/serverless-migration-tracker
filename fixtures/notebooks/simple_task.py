# Databricks notebook source
# Fixture: a clean notebook with nothing that blocks serverless (fake tables)
daily = spark.table("raw.events").groupBy("day").count()
daily.write.mode("overwrite").saveAsTable("curated.daily_counts")
