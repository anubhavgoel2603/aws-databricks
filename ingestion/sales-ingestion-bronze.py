# Databricks notebook source
# DBTITLE 1,Load CSV Data
def load_csv_data(csv_file_path):
  df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("inferSchema", "true")
        .load(csv_file_path)
    )
  displayHTML(f"<p>File from Path: {csv_file_path} loaded successfully into a dataframe</p>")
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
def load_to_bronze(df, catalog_name, schema_name, table_name):
    create_schema(catalog_name, schema_name)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    displayHTML(f"<p>Dataframe {df} loaded successfully to {catalog_name}.{schema_name}.{table_name}</p>")

# COMMAND ----------

# DBTITLE 1,Loading and Displaying Sales Invoices Data
df = load_csv_data("s3://joeysalesdata/bronze/invoices.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,Load Table to Bronze
load_to_bronze(df, "sales_catalog", "bronze", "raw_invoices")
