"""Gold layer business aggregations for sales."""

GOLD_VIEW = "dev.gold.sales"
SALES_BY_OUTLET_TABLE = "dev.gold.sales_by_outlet"
SALES_BY_ITEM_TYPE_TABLE = "dev.gold.sales_by_item_type"
TOP_ITEMS_BY_OUTLET_TABLE = "dev.gold.top_items_by_outlet"
OUTLET_SCORECARD_TABLE = "dev.gold.outlet_performance_scorecard"
MRP_SALES_ANALYSIS_TABLE = "dev.gold.mrp_sales_analysis"

spark.sql(
    """
    CREATE CATALOG IF NOT EXISTS dev
    MANAGED LOCATION 'abfss://gold@devstgaccdeprj.dfs.core.windows.net/'
    """
)

spark.sql("CREATE SCHEMA dev.gold MANAGED LOCATION 'abfss://gold@devstgaccdeprj.dfs.core.windows.net/'")

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {GOLD_VIEW}
    COMMENT 'Full cleansed and enriched Silver sales data — source for all Gold aggregations'
    AS
    SELECT * FROM dev.silver.sales
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SALES_BY_OUTLET_TABLE}
    COMMENT 'Sales revenue KPIs aggregated per outlet'
    AS
    SELECT
        Outlet_Identifier,
        Outlet_Type,
        Outlet_Size,
        Outlet_Location_Type,
        Outlet_Establishment_Year,
        Outlet_Age_Years,
        COUNT(*) AS total_items_stocked,
        COUNT(DISTINCT Item_Identifier) AS unique_items,
        ROUND(SUM(Item_Outlet_Sales), 2) AS total_sales,
        ROUND(AVG(Item_Outlet_Sales), 2) AS avg_sales_per_item,
        ROUND(MAX(Item_Outlet_Sales), 2) AS max_item_sales,
        ROUND(MIN(Item_Outlet_Sales), 2) AS min_item_sales,
        ROUND(STDDEV(Item_Outlet_Sales), 2) AS stddev_sales,
        ROUND(AVG(Item_MRP), 2) AS avg_mrp,
        current_timestamp() AS _gold_timestamp
    FROM dev.silver.sales
    GROUP BY
        Outlet_Identifier,
        Outlet_Type,
        Outlet_Size,
        Outlet_Location_Type,
        Outlet_Establishment_Year,
        Outlet_Age_Years
    ORDER BY total_sales DESC
    """
)

spark.sql(f"SELECT * FROM {SALES_BY_OUTLET_TABLE}").show(truncate=False)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SALES_BY_ITEM_TYPE_TABLE}
    COMMENT 'Sales performance aggregated by item category, fat content and price tier'
    AS
    SELECT
        Item_Type,
        Item_Fat_Content,
        MRP_Band,
        COUNT(*) AS total_records,
        COUNT(DISTINCT Item_Identifier) AS unique_items,
        ROUND(SUM(Item_Outlet_Sales), 2) AS total_sales,
        ROUND(AVG(Item_Outlet_Sales), 2) AS avg_sales,
        ROUND(PERCENTILE(Item_Outlet_Sales, 0.5), 2) AS median_sales,
        ROUND(AVG(Item_MRP), 2) AS avg_mrp,
        ROUND(AVG(Item_Weight), 2) AS avg_weight,
        ROUND(AVG(Item_Visibility), 4) AS avg_visibility,
        current_timestamp() AS _gold_timestamp
    FROM dev.silver.sales
    GROUP BY Item_Type, Item_Fat_Content, MRP_Band
    ORDER BY total_sales DESC
    """
)

spark.sql(f"SELECT * FROM {SALES_BY_ITEM_TYPE_TABLE}").show(truncate=False)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {TOP_ITEMS_BY_OUTLET_TABLE}
    COMMENT 'Top 10 revenue-generating items per outlet, ranked by item sales'
    AS
    WITH ranked AS (
        SELECT
            Outlet_Identifier,
            Outlet_Type,
            Outlet_Size,
            Outlet_Location_Type,
            Item_Identifier,
            Item_Type,
            Item_Fat_Content,
            ROUND(Item_MRP, 4) AS Item_MRP,
            MRP_Band,
            ROUND(Item_Outlet_Sales, 4) AS Item_Outlet_Sales,
            Sales_Band,
            RANK() OVER (
                PARTITION BY Outlet_Identifier
                ORDER BY Item_Outlet_Sales DESC
            ) AS sales_rank
        FROM dev.silver.sales
    )
    SELECT *
    FROM ranked
    WHERE sales_rank <= 10
    ORDER BY Outlet_Identifier, sales_rank
    """
)

spark.sql(
    f"SELECT * FROM {TOP_ITEMS_BY_OUTLET_TABLE} ORDER BY Outlet_Identifier, sales_rank"
).show(truncate=False)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {OUTLET_SCORECARD_TABLE}
    COMMENT 'Revenue and efficiency benchmarking grouped by outlet type, size and location tier'
    AS
    SELECT
        Outlet_Type,
        Outlet_Size,
        Outlet_Location_Type,
        COUNT(DISTINCT Outlet_Identifier) AS outlet_count,
        COUNT(*) AS total_items,
        ROUND(SUM(Item_Outlet_Sales), 2) AS total_sales,
        ROUND(AVG(Item_Outlet_Sales), 2) AS avg_sales_per_item,
        ROUND(SUM(Item_Outlet_Sales) / COUNT(DISTINCT Outlet_Identifier), 2) AS avg_revenue_per_outlet,
        ROUND(AVG(Outlet_Age_Years), 1) AS avg_outlet_age_years,
        current_timestamp() AS _gold_timestamp
    FROM dev.silver.sales
    GROUP BY Outlet_Type, Outlet_Size, Outlet_Location_Type
    ORDER BY total_sales DESC
    """
)

spark.sql(f"SELECT * FROM {OUTLET_SCORECARD_TABLE}").show(truncate=False)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {MRP_SALES_ANALYSIS_TABLE}
    COMMENT 'Cross-analysis of price tier, visibility band and fat content vs actual sales performance'
    AS
    SELECT
        MRP_Band,
        Sales_Band,
        Item_Visibility_Band,
        Item_Fat_Content,
        COUNT(*) AS records,
        ROUND(AVG(Item_MRP), 2) AS avg_mrp,
        ROUND(AVG(Item_Outlet_Sales), 2) AS avg_sales,
        ROUND(SUM(Item_Outlet_Sales), 2) AS total_sales,
        ROUND(AVG(Item_Outlet_Sales) / NULLIF(AVG(Item_MRP), 0), 4) AS avg_sales_to_mrp_ratio,
        current_timestamp() AS _gold_timestamp
    FROM dev.silver.sales
    GROUP BY MRP_Band, Sales_Band, Item_Visibility_Band, Item_Fat_Content
    ORDER BY total_sales DESC
    """
)

spark.sql(f"SELECT * FROM {MRP_SALES_ANALYSIS_TABLE}").show(truncate=False)

assets = {
    GOLD_VIEW: "VIEW",
    SALES_BY_OUTLET_TABLE: "TABLE",
    SALES_BY_ITEM_TYPE_TABLE: "TABLE",
    TOP_ITEMS_BY_OUTLET_TABLE: "TABLE",
    OUTLET_SCORECARD_TABLE: "TABLE",
    MRP_SALES_ANALYSIS_TABLE: "TABLE",
}

print("=== Gold Layer Catalog ===")
print(f"\n  {'Asset':<45} {'Type':<8} {'Rows':>8}   {'Cols':>5}")
print("  " + "-" * 72)
for asset_name, asset_type in assets.items():
    asset_df = spark.table(asset_name)
    print(f"  {asset_name:<45} {asset_type:<8} {asset_df.count():>8,}   {len(asset_df.columns):>5}")
