# Databricks notebook source
# DBTITLE 1,Load CSV Data
def load_table_data(catalog_name, schema_name, table_name):
  df = (
        spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")
    )
  displayHTML(f"<p>Dataframe loaded successfully from {catalog_name}.{schema_name}.{table_name}</p>")
  return df

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

def transform_invoice_data(df):
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "d-M-y H.m")) \
           .withColumn("CustomerID", col("CustomerID").cast("string"))
    return df

# COMMAND ----------

# DBTITLE 1,Create Schema
def create_schema(catalog_name, schema_name):
    spark.sql(f"USE CATALOG {catalog_name}")
    return (
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        )

# COMMAND ----------

# DBTITLE 1,Load DataFrame to Bronze Table
def load_to_silver(df, catalog_name, schema_name, table_name):
    create_schema(catalog_name, schema_name)
    df.write.format("delta").mode("overwrite").partitionBy("Country","InvoiceDate")\
    .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    displayHTML(f"<p>Dataframe {df} loaded successfully to silver layer table {catalog_name}.{schema_name}.{table_name}</p>")

# COMMAND ----------

# DBTITLE 1,Loading and Displaying Sales Invoices Data
raw_df = load_table_data("sales_catalog", "bronze", "raw_invoices")
transformed_df = transform_invoice_data(raw_df)
load_to_silver(transformed_df, "sales_catalog", "silver", "invoices")
