-- Databricks notebook source
-- Create Streaming Table

CREATE OR REPLACE STREAMING TABLE st_orders
AS
SELECT *
FROM STREAM(samples.tpch.orders);

-- COMMAND ----------

-- Create Materialized View

CREATE OR REPLACE MATERIALIZED VIEW agg_orders
AS
SELECT 
  o_orderstatus,
  COUNT(o_orderkey) AS cnt_orders
FROM LIVE.st_orders
GROUP BY o_order_status;
