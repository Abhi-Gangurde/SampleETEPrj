"""Bronze layer raw data ingestion for sales."""

from pyspark.sql.functions import col

RAW_FILE_PATH = "abfss://raw@devstgaccdeprj.dfs.core.windows.net/Sales.parquet"
BRONZE_TABLE = "dev.bronze.sales"

spark.sql(
    """
    CREATE CATALOG IF NOT EXISTS dev
    MANAGED LOCATION 'abfss://raw@devstgaccdeprj.dfs.core.windows.net/'
    """
)

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.bronze")

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {BRONZE_TABLE}
    AS
    SELECT
        *,
        current_timestamp() AS _ingestion_timestamp,
        current_user() AS _ingested_by,
        '{RAW_FILE_PATH}' AS _source_file,
        'Sales.parquet' AS _source_filename,
        'ADLS Gen2' AS _source_system
    FROM PARQUET.`{RAW_FILE_PATH}`
    """
)

# Row count and schema validation
bronze_df = spark.table(BRONZE_TABLE)
row_count = bronze_df.count()
col_count = len(bronze_df.columns)

print(f"Table     : {BRONZE_TABLE}")
print(f"Row Count : {row_count:,}")
print(f"Columns   : {col_count}")
print()
bronze_df.printSchema()

# Null and empty-value analysis per column
record_count = bronze_df.count()
data_cols = [column_name for column_name in bronze_df.columns if not column_name.startswith("_")]

results = []
for column_name in data_cols:
    null_count = bronze_df.filter(col(column_name).isNull() | (col(column_name) == "")).count()
    results.append(
        {
            "column": column_name,
            "null_or_empty": null_count,
            "null_pct": round(null_count / record_count * 100, 2),
        }
    )

results.sort(key=lambda row: row["null_or_empty"], reverse=True)

print(f"{'Column':<35} {'Null/Empty':>12} {'%':>8}")
print("-" * 58)
for result in results:
    flag = " *" if result["null_pct"] > 0 else ""
    print(
        f"{result['column']:<35} {result['null_or_empty']:>12,} "
        f"{result['null_pct']:>7.2f}%{flag}"
    )

# Bronze data statistics
spark.sql(
    f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT Item_Identifier) AS unique_items,
        COUNT(DISTINCT Outlet_Identifier) AS unique_outlets,
        ROUND(AVG(CAST(Item_Outlet_Sales AS DOUBLE)), 2) AS avg_sales,
        ROUND(MIN(CAST(Item_Outlet_Sales AS DOUBLE)), 2) AS min_sales,
        ROUND(MAX(CAST(Item_Outlet_Sales AS DOUBLE)), 2) AS max_sales,
        MIN(CAST(Outlet_Establishment_Year AS INT)) AS oldest_outlet_year,
        MAX(CAST(Outlet_Establishment_Year AS INT)) AS newest_outlet_year,
        COUNT(CASE WHEN Item_Weight IS NULL THEN 1 END) AS null_item_weight,
        COUNT(CASE WHEN Outlet_Size IS NULL THEN 1 END) AS null_outlet_size,
        MIN(_ingestion_timestamp) AS ingestion_timestamp
    FROM {BRONZE_TABLE}
    """
).show(truncate=False)

# Categorical column distinct values
categorical_cols = [
    "Item_Fat_Content",
    "Item_Type",
    "Outlet_Identifier",
    "Outlet_Size",
    "Outlet_Location_Type",
    "Outlet_Type",
]

print(f"{'Column':<30} {'#Distinct':>10}   Values")
print("-" * 80)
for column_name in categorical_cols:
    values = sorted(
        [row[column_name] for row in bronze_df.select(column_name).distinct().collect() if row[column_name] is not None]
    )
    print(f"{column_name:<30} {len(values):>10}   {values}")
