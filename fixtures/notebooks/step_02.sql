-- Databricks notebook source
CACHE TABLE sales_cache AS SELECT * FROM raw.sales;

-- COMMAND ----------

SELECT CAST(amount AS DECIMAL(10,2)) AS amt,
       revenue / units AS unit_price,
       code::int AS code_int
FROM sales_cache;

-- COMMAND ----------

UNCACHE TABLE sales_cache;
