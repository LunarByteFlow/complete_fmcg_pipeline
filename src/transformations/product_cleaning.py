"""
product_cleaning.py
-------------------
Standardizes product dimensions, fixes typos, and generates product codes.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_product_data(df_raw: DataFrame) -> DataFrame:
    df_clean = df_raw.dropDuplicates(['product_id'])
    
    # Fix typos & case
    df_clean = df_clean.withColumn("category", F.initcap(F.col("category"))) \
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")) \
        .withColumn("category", F.regexp_replace(F.col("category"), "(?i)Protien", "Protein"))
        
    # Map divisions & extract variants
    df_clean = df_clean.withColumn(
        "division",
        F.when(F.col("category").isin(["Energy Bars", "Protein Bars"]), "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks", "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
         .otherwise("Other")
    ).withColumn("variant", F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1))
    
    # Generate SHA-256 product_code and handle invalid product_ids
    return df_clean.withColumn("product_code", F.sha2(F.col("product_name").cast("string"), 256)) \
        .withColumn("is_valid_product_id", F.col("product_id").cast("string").rlike("^[0-9]+$")) \
        .withColumnRenamed("product_name", "product")