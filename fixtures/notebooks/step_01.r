# Databricks notebook source
# Fixture: R notebook that depends on a CRAN package (fake data path)
library(randomForest)
df <- read.csv("/dbfs/data/example.csv")
model <- randomForest(y ~ ., data = df)
