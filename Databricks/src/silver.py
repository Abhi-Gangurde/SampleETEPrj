"""Silver layer cleansing and enrichment for sales."""

from pyspark.sql.functions import col

SILVER_TABLE = "dev.silver.sales"

spark.sql(
    """
    CREATE CATALOG IF NOT EXISTS dev
    MANAGED LOCATION 'abfss://silver@devstgaccdeprj.dfs.core.windows.net/'
    """
)

spark.sql("CREATE SCHEMA dev.silver MANAGED LOCATION 'abfss://silver@devstgaccdeprj.dfs.core.windows.net/'")

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE}
    AS
    WITH
    typed AS (
        SELECT
            Item_Identifier,
            CAST(Item_Weight AS DOUBLE) AS Item_Weight,
            CASE UPPER(TRIM(Item_Fat_Content))
                WHEN 'LF' THEN 'Low Fat'
                WHEN 'LOW FAT' THEN 'Low Fat'
                WHEN 'REG' THEN 'Regular'
                WHEN 'REGULAR' THEN 'Regular'
                ELSE INITCAP(TRIM(Item_Fat_Content))
            END AS Item_Fat_Content,
            CAST(Item_Visibility AS DOUBLE) AS Item_Visibility,
            TRIM(Item_Type) AS Item_Type,
            CAST(Item_MRP AS DOUBLE) AS Item_MRP,
            Outlet_Identifier,
            CAST(Outlet_Establishment_Year AS INT) AS Outlet_Establishment_Year,
            COALESCE(NULLIF(TRIM(Outlet_Size), ''), 'Unknown') AS Outlet_Size,
            TRIM(Outlet_Location_Type) AS Outlet_Location_Type,
            TRIM(Outlet_Type) AS Outlet_Type,
            CAST(Item_Outlet_Sales AS DOUBLE) AS Item_Outlet_Sales,
            _ingestion_timestamp,
            _source_filename,
            _source_system
        FROM dev.bronze.sales
    ),
    weight_stats AS (
        SELECT ROUND(AVG(Item_Weight), 4) AS mean_item_weight
        FROM typed
        WHERE Item_Weight IS NOT NULL
    ),
    deduped AS (
        SELECT
            typed.*,
            ROW_NUMBER() OVER (
                PARTITION BY Item_Identifier, Outlet_Identifier
                ORDER BY _ingestion_timestamp DESC
            ) AS _rn
        FROM typed
    )
    SELECT
        deduped.Item_Identifier,
        ROUND(COALESCE(deduped.Item_Weight, weight_stats.mean_item_weight), 4) AS Item_Weight,
        deduped.Item_Fat_Content,
        ROUND(deduped.Item_Visibility, 6) AS Item_Visibility,
        deduped.Item_Type,
        ROUND(deduped.Item_MRP, 4) AS Item_MRP,
        deduped.Outlet_Identifier,
        deduped.Outlet_Establishment_Year,
        deduped.Outlet_Size,
        deduped.Outlet_Location_Type,
        deduped.Outlet_Type,
        ROUND(deduped.Item_Outlet_Sales, 4) AS Item_Outlet_Sales,
        YEAR(CURRENT_DATE()) - deduped.Outlet_Establishment_Year AS Outlet_Age_Years,
        CASE
            WHEN deduped.Item_Visibility = 0 THEN 'Not Visible'
            WHEN deduped.Item_Visibility < 0.05 THEN 'Low'
            WHEN deduped.Item_Visibility < 0.15 THEN 'Medium'
            ELSE 'High'
        END AS Item_Visibility_Band,
        CASE
            WHEN deduped.Item_Outlet_Sales < 500 THEN 'Low'
            WHEN deduped.Item_Outlet_Sales < 2000 THEN 'Medium'
            WHEN deduped.Item_Outlet_Sales < 5000 THEN 'High'
            ELSE 'Very High'
        END AS Sales_Band,
        CASE
            WHEN deduped.Item_MRP < 50 THEN 'Budget'
            WHEN deduped.Item_MRP < 100 THEN 'Economy'
            WHEN deduped.Item_MRP < 200 THEN 'Mid-Range'
            ELSE 'Premium'
        END AS MRP_Band,
        current_timestamp() AS _silver_timestamp,
        current_user() AS _silver_processed_by,
        deduped._ingestion_timestamp AS _bronze_ingestion_timestamp,
        deduped._source_filename,
        deduped._source_system
    FROM deduped
    CROSS JOIN weight_stats
    WHERE deduped._rn = 1
    """
)

# Bronze vs Silver row count and deduplication report
bronze_df = spark.table("dev.bronze.sales")
silver_df = spark.table(SILVER_TABLE)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
dedup_removed = bronze_count - silver_count

print("=== Bronze to Silver Processing Summary ===")
print(f"  Bronze rows     : {bronze_count:,}")
print(f"  Silver rows     : {silver_count:,}")
print(f"  Removed (dedup) : {dedup_removed:,}")
print(f"  Bronze columns  : {len(bronze_df.columns)}")
print(f"  Silver columns  : {len(silver_df.columns)}")
print("  New derived cols: Outlet_Age_Years, Item_Visibility_Band, Sales_Band, MRP_Band")

# Post-Silver null analysis
total_rows = silver_df.count()
data_cols = [column_name for column_name in silver_df.columns if not column_name.startswith("_")]

print(f"Post-Silver Null Analysis  (total rows: {total_rows:,})\n")
print(f"  {'Column':<35} {'Nulls':>8}   {'%':>6}")
print("  " + "-" * 55)
any_null = False
for column_name in data_cols:
    null_count = silver_df.filter(col(column_name).isNull()).count()
    null_pct = null_count / total_rows * 100
    flag = " *" if null_count > 0 else ""
    if null_count > 0:
        any_null = True
    print(f"  {column_name:<35} {null_count:>8,}   {null_pct:>5.2f}%{flag}")
if not any_null:
    print("\n  No nulls remaining in any data column.")

# Item_Fat_Content standardization verification
spark.sql(
    f"""
    SELECT
        Item_Fat_Content,
        COUNT(*) AS record_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {SILVER_TABLE}
    GROUP BY Item_Fat_Content
    ORDER BY record_count DESC
    """
).show(truncate=False)

# Outlet_Size distribution after null imputation
spark.sql(
    f"""
    SELECT
        Outlet_Size,
        COUNT(*) AS record_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {SILVER_TABLE}
    GROUP BY Outlet_Size
    ORDER BY record_count DESC
    """
).show(truncate=False)

# Derived column distributions
spark.sql(
    f"""
    SELECT
        Sales_Band,
        MRP_Band,
        Item_Visibility_Band,
        COUNT(*) AS records,
        ROUND(AVG(Item_Outlet_Sales), 2) AS avg_sales,
        ROUND(AVG(Item_MRP), 2) AS avg_mrp
    FROM {SILVER_TABLE}
    GROUP BY Sales_Band, MRP_Band, Item_Visibility_Band
    ORDER BY Sales_Band, MRP_Band
    """
).show(truncate=False)

# Silver data quality scorecard
checks = {
    "No null Item_Weight (imputed)": silver_df.filter(col("Item_Weight").isNull()).count() == 0,
    "No null Outlet_Size (Unknown filled)": silver_df.filter(col("Outlet_Size").isNull()).count() == 0,
    "Item_Fat_Content only Low Fat/Regular": silver_df.filter(
        ~col("Item_Fat_Content").isin("Low Fat", "Regular")
    ).count() == 0,
    "Item_MRP > 0": silver_df.filter(col("Item_MRP") <= 0).count() == 0,
    "Item_Outlet_Sales > 0": silver_df.filter(col("Item_Outlet_Sales") <= 0).count() == 0,
    "Item_Visibility >= 0": silver_df.filter(col("Item_Visibility") < 0).count() == 0,
    "Outlet_Age_Years >= 0": silver_df.filter(col("Outlet_Age_Years") < 0).count() == 0,
    "No duplicates (Item + Outlet key)": silver_df.count()
    == silver_df.dropDuplicates(["Item_Identifier", "Outlet_Identifier"]).count(),
}

passed = sum(1 for result in checks.values() if result)
score = passed / len(checks) * 100

print(f"=== Silver Data Quality Score: {score:.0f}%  ({passed}/{len(checks)} passed) ===\n")
for check_name, check_result in checks.items():
    status = "PASS" if check_result else "FAIL"
    print(f"  {status:<4}   {check_name}")

# Delta table metadata
spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}").show(truncate=False)
