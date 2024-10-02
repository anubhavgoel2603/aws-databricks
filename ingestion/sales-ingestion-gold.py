# Databricks notebook source
# DBTITLE 1,Load Silver Data
def load_table_data(catalog_name, schema_name, table_name):
  df = (
        spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")
    )
  displayHTML(f"<p>Dataframe loaded successfully from {catalog_name}.{schema_name}.{table_name}</p>")
  return df

# COMMAND ----------

def transform_invoice_data(df, view_name):
    df.createOrReplaceTempView(view_name)
    df = spark.sql(f"SELECT Country, CustomerID, DATEFORMAT(InvoiceDate, 'yyyy-MM-dd HH:mm:ss') AS InvoiceDate, sum(Quantity) AS Total_Quantity, sum(UnitPrice * Quantity) AS Total_Price FROM {view_name} GROUP BY Country, CustomerID, InvoiceDate")
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
def load_to_gold(df, catalog_name, schema_name, table_name):
    create_schema(catalog_name, schema_name)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    displayHTML(f"<p>Dataframe {df} loaded successfully to silver layer table {catalog_name}.{schema_name}.{table_name}</p>")

# COMMAND ----------

# DBTITLE 1,Loading and Displaying Sales Invoices Data
silver_df = load_table_data("sales_catalog", "silver", "invoices")
transformed_df = transform_invoice_data(silver_df,"invoices_temp_view")
load_to_gold(transformed_df, "sales_catalog", "gold", "final_invoices")
