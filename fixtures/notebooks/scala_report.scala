// Databricks notebook source
// Fixture: small DataFrame-only Scala notebook (fake tables)
val sales = spark.table("raw.sales")
val report = sales.groupBy("region").sum("amount")
report.write.mode("overwrite").saveAsTable("reports.sales_by_region")
