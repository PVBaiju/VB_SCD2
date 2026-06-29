
1. Mount ADLS Containers (if not already mounted)================================
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "Client id",
  "fs.azure.account.oauth2.client.secret": "Secret value od app",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/TenantID/oauth2/token"
}
dbutils.fs.mount(
  source = "abfss://output@adlsdevvbsrc01.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

dbutils.fs.mount(
  source = "abfss://output@adlsdevvbstd01.dfs.core.windows.net/",
  mount_point = "/mnt/curated",
  extra_configs = configs
)


2. Read CSV from Raw Container==============================

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/raw/event_trigger_csv/2025/08/27/customers_dataset.csv")

display(raw_df)

3. Write Initial Delta Table
raw_df.write.format("delta").mode("overwrite").save("/mnt/curated/customers_delta")
spark.sql("CREATE TABLE IF NOT EXISTS customers_delta USING DELTA LOCATION '/mnt/curated/customers_delta'")

4. SCD Type 1 (Overwrite Old Values)

👉 In SCD1, the new data replaces the old records.

# New incoming data
new_df = raw_df.withColumn("ingest_date", current_date())

# Merge for SCD1 (overwrite)
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/curated/customers_delta")

(deltaTable.alias("t")
 .merge(new_df.alias("s"), "t.customer_id = s.customer_id")
 .whenMatchedUpdateAll()  # overwrite existing values
 .whenNotMatchedInsertAll()  # insert new records
 .execute())


5. SCD Type 2 (Maintain History)

👉 In SCD2, new records close out old versions and insert a new version with effective_start_date, effective_end_date, and is_active.
from pyspark.sql.functions import lit, current_date, col

# Add SCD2 columns
scd2_new = raw_df.withColumn("effective_start_date", current_date()) \
                 .withColumn("effective_end_date", lit("9999-12-31")) \
                 .withColumn("is_active", lit(True))

deltaTable = DeltaTable.forPath(spark, "/mnt/curated/customers_scd2")

# Merge with SCD2 logic
(deltaTable.alias("t")
 .merge(scd2_new.alias("s"), "t.customer_id = s.customer_id AND t.is_active = True")
 .whenMatchedUpdate(set={
     "effective_end_date": current_date(),
     "is_active": lit(False)
 })
 .whenNotMatchedInsertAll()
 .execute())
===================================================================================================================
from pyspark.sql.functions import current_date, lit
from delta.tables import DeltaTable

# Add SCD2 fields to incoming dataframe
new_df = (raw_df
          .withColumn("ingest_date", current_date())
          .withColumn("valid_from", current_date())
          .withColumn("valid_to", lit(None).cast("date"))
          .withColumn("is_current", lit(True)))

# Get Delta table reference
delta_path = "/mnt/curated/customers_delta_scd2"
deltaTable = DeltaTable.forPath(spark, delta_path)

# SCD2 merge logic
(
    deltaTable.alias("t")
    .merge(new_df.alias("s"), "t.customer_id = s.customer_id AND t.is_current = True")
    .whenMatchedUpdate(
        condition="t.customer_city <> s.customer_city OR t.customer_state <> s.customer_state",
        set={
            "valid_to": "current_date()",
            "is_current": "False"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# Reload updated table
updated_df = spark.read.format("delta").load(delta_path)
updated_df.show(20, truncate=False)
===================================================================================================================


-- SCD1 result
SELECT * FROM customers_delta;

-- SCD2 history tracking
SELECT * FROM customers_scd2 ORDER BY customer_id, effective_start_date;
========================================================================USING - Creating DB============================================================================================================================

Option 1: Use Delta Tables on Storage Only

In Databricks, you can directly save Delta tables to your ADLS location:
/mnt/curated/customers_delta
/mnt/curated/customers_scd2

Option 2: Create a Database (Schema) in Databricks

If you want things organized, you can create a database (schema) and register your Delta tables there:

CREATE DATABASE IF NOT EXISTS ecommerce_etl LOCATION '/mnt/curated/ecommerce_etl';

CREATE TABLE ecommerce_etl.customers_delta
USING DELTA
LOCATION '/mnt/curated/customers_delta';

CREATE TABLE ecommerce_etl.customers_scd2
USING DELTA
LOCATION '/mnt/curated/customers_scd2';


Benefit: now your users (analysts, BI tools) can simply query:
SELECT * FROM ecommerce_etl.customers_delta;

===============================================================SCD1 Working=====================================================================================================================================
from pyspark.sql.functions import current_date
from delta.tables import DeltaTable

delta_path = "/mnt/curated/customers_delta"

# Add ingest_date to incoming dataframe
new_df = raw_df.withColumn("ingest_date", current_date())

if not DeltaTable.isDeltaTable(spark, delta_path):
    # Initial load (creates the Delta table if it doesn’t exist)
    (new_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(delta_path))
else:
    # Merge (updates + inserts)
    deltaTable = DeltaTable.forPath(spark, delta_path)
    (deltaTable.alias("t")
        .merge(new_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    
# Reload with schema evolution enabled
updated_df = spark.read.format("delta").option("mergeSchema", "true").load("/mnt/curated/customers_delta")

updated_df.show(10, truncate=False)

print("Before Merge:")
raw_df.show(10, truncate=False)

print("After Merge:")
updated_df.show(10, truncate=False)
===============================================================SCD2 Working=====================================================================================================================================