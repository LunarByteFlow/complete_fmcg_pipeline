"""
customer_cleaning.py
--------------------
Cleans customer records, fixes city typos, and builds standardized attributes.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CITY_MAPPING = {
    'Bengaluruu': 'Bengaluru', 'Bengalore': 'Bengaluru',
    'Hyderabadd': 'Hyderabad', 'Hyderbad': 'Hyderabad',
    'NewDelhi': 'New Delhi', 'NewDheli': 'New Delhi', 'NewDelhee': 'New Delhi'
}
ALLOWED_CITIES = ['Bengaluru', 'Hyderabad', 'New Delhi']

def clean_customer_data(df_raw: DataFrame, df_fix: DataFrame = None) -> DataFrame:
    # 1. Deduplicate & trim whitespace
    df_clean = df_raw.dropDuplicates(['customer_id'])
    df_clean = df_clean.withColumn("customer_name", F.trim(F.col("customer_name")))
    
    # 2. Fix city typos & standardize case
    df_clean = df_clean.replace(CITY_MAPPING, subset=['city'])
    df_clean = df_clean.withColumn(
        'city',
        F.when(F.col('city').isin(ALLOWED_CITIES), F.col('city')).otherwise(None)
    )
    df_clean = df_clean.withColumn('customer_name', F.initcap(F.col('customer_name')))
    
    # 3. Apply manual business fixes for missing cities if fix DataFrame is provided
    if df_fix:
        df_clean = df_clean.join(df_fix, 'customer_id', 'left') \
            .withColumn('city', F.coalesce('city', 'fixed_city')) \
            .drop('fixed_city')
            
    # 4. Add parent company attributes
    return df_clean.withColumn('customer_id', F.col('customer_id').cast('string')) \
        .withColumn('customer', F.concat_ws('-', 'customer_name', F.coalesce(F.col('city'), F.lit('Unknown')))) \
        .withColumn('market', F.lit('India')) \
        .withColumn('platform', F.lit('Sports Bar')) \
        .withColumn('channel', F.lit('Acquisition'))