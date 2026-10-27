# Databricks notebook source
# Fixture: legacy notebook (fake paths, no real data)
dbutils.fs.mount(source="s3a://example-bucket", mount_point="/mnt/example")
df = spark.read.parquet("/mnt/example/input")
total = df.rdd.map(lambda r: 1).sum()
