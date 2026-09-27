"""
pricing_cleaning.py
--------------------
Transformation module for cleansing and validating raw product pricing
records (gross_price) for the FMCG Silver and Gold layers.
"""
 
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
 
def clean_pricing_data(df_raw: DataFrame, df_products: DataFrame) -> DataFrame:
    """
    Cleans raw gross_price records by normalising mixed date formats,
    validating and sign-correcting price values, and enriching with
    product_code via a join against the Silver products table.
 
    Args:
        df_raw (DataFrame): Raw Bronze pricing records.
        df_products (DataFrame): Silver products table, used to resolve
            product_code from product_id.
 
    Returns:
        DataFrame: Cleaned Silver-grain pricing records, one row per
            product_code / month, with a validated gross_price column.
    """
    # 1. Parse `month` from multiple possible formats
    df_clean = df_raw.withColumn(
        "month",
        F.coalesce(
            F.try_to_date(F.col("month"), "yyyy/MM/dd"),
            F.try_to_date(F.col("month"), "dd/MM/yyyy"),
            F.try_to_date(F.col("month"), "yyyy-MM-dd"),
            F.try_to_date(F.col("month"), "dd-MM-yyyy"),
        ),
    )
 
    # 2. Validate gross_price: keep valid numerics, flip negatives positive,
    #    replace anything non-numeric with 0
    df_clean = df_clean.withColumn(
        "gross_price",
        F.when(
            F.col("gross_price").rlike(r"^-?\d+(\.\d+)?$"),
            F.when(
                F.col("gross_price").cast("double") < 0,
                -1 * F.col("gross_price").cast("double"),
            ).otherwise(F.col("gross_price").cast("double")),
        ).otherwise(0),
    )
 
    # 3. Enrich with product_code via inner join against Silver products
    df_joined = df_clean.join(
        df_products.select("product_id", "product_code"),
        on="product_id",
        how="inner",
    ).select(
        "product_id",
        "product_code",
        "month",
        "gross_price",
        "read_timestamp",
        "file_name",
        "file_size",
    )
 
    return df_joined
 
 
def build_latest_price_per_year(df_silver_pricing: DataFrame) -> DataFrame:
    """
    Reduces Silver-grain pricing (one row per product_code/month) down to
    one row per product_code/year, preferring the latest non-zero price
    observed that year.
 
    Args:
        df_silver_pricing (DataFrame): Output of clean_pricing_data.
 
    Returns:
        DataFrame: product_code, price_inr, year (string) -- ready for
            merge into the Gold dim_gross_price table.
    """
    df_priced = df_silver_pricing.withColumn(
        "year", F.year("month")
    ).withColumn(
        # 0 = non-zero price, 1 = zero price -> non-zero ranks first
        "is_zero",
        F.when(F.col("gross_price") == 0, 1).otherwise(0),
    )
 
    window_spec = (
        Window.partitionBy("product_id", "year")
        .orderBy(F.col("is_zero"), F.col("month").desc())
    )
 
    df_latest = (
        df_priced.withColumn("rnk", F.row_number().over(window_spec))
        .filter(F.col("rnk") == 1)
        .select("product_id", "product_code", "year", "gross_price")
        .withColumnRenamed("gross_price", "price_inr")
        .withColumn("year", F.col("year").cast("string"))
    )
 
    return df_latest
 
