"""
Streaming ETL Consumer for Altman Z-Score and Z'-Score.
Features:
- Dual-Scoring: Evaluates both Original and Prime formulas simultaneously.
- Data Quality Gate (DLQ): Filters invalid raw data and writes to native logger.
- Anti-Poisoning: Sanitizes NaNs and prevents Division by Zero in financial math.
- Silver/Gold Materialization: Calculates global market averages and Top-5 Leaderboards.
"""

import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, isnan
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType

# Import the native logger from your project structure
from src.raw_features.logger import log_message

# --- CONFIGURATION CONSTANTS ---
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "raw_features" 
APP_NAME = "Altman_Dual_Scoring_ETL"
SILVER_STORAGE_PATH = "local_storage/silver_scores"

def clean_silver_storage():
    """
    Cleans the local Parquet storage directory before starting the stream.
    Ensures a fresh start for the demonstration.
    """
    if os.path.exists(SILVER_STORAGE_PATH):
        shutil.rmtree(SILVER_STORAGE_PATH)
        log_message(f"Cleared previous state at {SILVER_STORAGE_PATH}", "INFO")

def get_schema() -> StructType:
    return StructType([
        StructField("ticker", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("common_stock_units", FloatType(), True),
        StructField("current_assets", FloatType(), True),
        StructField("current_liabilities", FloatType(), True),
        StructField("short_term_debt", FloatType(), True),
        StructField("long_term_debt", FloatType(), True),
        StructField("retained_earnings", FloatType(), True),
        StructField("stockholders_equity", FloatType(), True),
        StructField("total_assets", FloatType(), True),
        StructField("net_income", FloatType(), True),
        StructField("interest_expense", FloatType(), True),
        StructField("tax_expense", FloatType(), True),
        StructField("total_revenue", FloatType(), True),
        StructField("price", StringType(), True), 
        StructField("units", MapType(StringType(), StringType()), True)
    ])

def data_quality_gate(df: DataFrame) -> DataFrame:
    """
    Evaluates incoming raw data for completeness before applying math.
    Flags records with nulls/NaNs in critical baseline fields as invalid.
    """
    critical_fields = [
        "current_assets", "current_liabilities", "total_assets", 
        "retained_earnings", "stockholders_equity", "net_income", "total_revenue"
    ]
    
    is_valid_cond = col("ticker").isNotNull() & col("year").isNotNull()
    
    for field in critical_fields:
        is_valid_cond = is_valid_cond & col(field).isNotNull() & ~isnan(col(field))
        
    # Total Assets must be > 0 to prevent base Division By Zero
    is_valid_cond = is_valid_cond & (col("total_assets") > 0)
    
    return df.withColumn("is_valid_record", is_valid_cond)

def preprocess_and_engineer_features(df: DataFrame) -> DataFrame:
    """
    Sanitizes values, applies multipliers, and computes derived financial metrics.
    """
    # 1. Sanitize Price
    df = df.withColumn(
        "clean_price", 
        when(col("price") == "N/A", lit(None).cast("float"))
        .otherwise(col("price").cast("float"))
    )

    # 2. Extract Unit Multiplier
    df = df.withColumn("unit_str", col("units").getItem("current_assets"))
    df = df.withColumn(
        "multiplier",
        when(col("unit_str").rlike("(?i)millions"), lit(1_000_000.0))
        .when(col("unit_str").rlike("(?i)thousands"), lit(1_000.0))
        .otherwise(lit(1_000_000.0))
    )

    fields_to_multiply = [
        "common_stock_units", "current_assets", "current_liabilities", 
        "short_term_debt", "long_term_debt", "retained_earnings", 
        "stockholders_equity", "total_assets", "net_income", 
        "interest_expense", "tax_expense", "total_revenue"
    ]
    
    # 3. Apply Multiplier and Bulletproof against missing secondary metrics (e.g., 0 debt)
    for field in fields_to_multiply:
        df = df.withColumn(field, 
            when(col(field).isNull() | isnan(col(field)), lit(0.0))
            .otherwise(col(field)) * col("multiplier")
        )

    # 4. Feature Engineering
    df = df.withColumn("total_liabilities", col("current_liabilities") + col("long_term_debt") + col("short_term_debt"))
    df = df.withColumn("ebit", col("net_income") + col("interest_expense") + col("tax_expense"))
    df = df.withColumn("market_cap", col("common_stock_units") * col("clean_price"))
    
    return df

def calculate_dual_z_scores(df: DataFrame) -> DataFrame:
    """
    Computes Z-Score formulas using safe division. 
    Applies final Anti-Poison filters to drop lingering NaNs.
    """
    # 1. Standard Ratios
    df = df.withColumn("X1", (col("current_assets") - col("current_liabilities")) / col("total_assets")) \
           .withColumn("X2", col("retained_earnings") / col("total_assets")) \
           .withColumn("X3", col("ebit") / col("total_assets")) \
           .withColumn("X5", col("total_revenue") / col("total_assets"))

    # 2. Safe Division for X4 (Handles companies with exactly 0 total liabilities)
    df = df.withColumn("X4_Original", 
        when(col("total_liabilities") > 0, col("market_cap") / col("total_liabilities"))
        .otherwise(col("market_cap"))
    )
    df = df.withColumn("X4_Prime", 
        when(col("total_liabilities") > 0, col("stockholders_equity") / col("total_liabilities"))
        .otherwise(col("stockholders_equity"))
    )

    # 3. Calculate Scores
    df = df.withColumn(
        "Z_Score_Original",
        round((lit(1.2) * col("X1")) + (lit(1.4) * col("X2")) + (lit(3.3) * col("X3")) + (lit(0.6) * col("X4_Original")) + (lit(1.0) * col("X5")), 2)
    )

    df = df.withColumn(
        "Z_Score_Prime",
        round((lit(0.717) * col("X1")) + (lit(0.847) * col("X2")) + (lit(3.107) * col("X3")) + (lit(0.420) * col("X4_Prime")) + (lit(0.998) * col("X5")), 2)
    )

    # 4. Assign Risk Zones
    df = df.withColumn(
        "Health_Zone_Original",
        when(col("Z_Score_Original").isNull(), lit("N/A - No Market Cap"))
        .when(col("Z_Score_Original") >= 2.99, "Safe (Green)")
        .when(col("Z_Score_Original") <= 1.81, "Distress (Red)")
        .otherwise("Grey (Caution)")
    )

    df = df.withColumn(
        "Health_Zone_Prime",
        when(col("Z_Score_Prime") >= 2.90, "Safe (Green)")
        .when(col("Z_Score_Prime") <= 1.23, "Distress (Red)")
        .otherwise("Grey (Caution)")
    )

    # Clean intermediate logic and apply Final Anti-Poison Filter
    df = df.drop("X1", "X2", "X3", "X5", "X4_Original", "X4_Prime")
    return df.filter(~isnan(col("Z_Score_Prime")))

def process_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Executes for every micro-batch. Routes invalid records to DLQ logs 
    and appends valid records to Silver storage to update Gold dashboards.
    """
    record_count = batch_df.count()
    if record_count == 0:
        return

    log_message(f"[Batch {batch_id}] Intercepted {record_count} raw records from Kafka.", "INFO")

    # --- ROUTING: DATA QUALITY GATE ---
    bad_df = batch_df.filter(col("is_valid_record") == False)
    good_df = batch_df.filter(col("is_valid_record") == True).drop("is_valid_record")

    bad_count = bad_df.count()
    good_count = good_df.count()

    # --- DEAD LETTER LOGGING (Handling Corrupted Data) ---
    if bad_count > 0:
        log_message(f"[Batch {batch_id}] DATA QUALITY ALERT: Dropping {bad_count} invalid records.", "WARNING")
        corrupted_rows = bad_df.select("ticker", "year").collect()
        for row in corrupted_rows:
            log_message(f"[Batch {batch_id}] REJECTED: Ticker '{row.ticker}' for Year '{row.year}'. Reason: Missing/NaN critical financial fields or Total Assets <= 0.", "WARNING")

    # --- DASHBOARD GENERATION (Handling Clean Data) ---
    if good_count > 0:
        log_message(f"[Batch {batch_id}] Proceeding with {good_count} valid records.", "INFO")
        
        print(f"\n=======================================================")
        print(f"📥 [BATCH {batch_id}] NEW VALID STREAM DATA")
        print(f"=======================================================")
        good_df.select("ticker", "year", "Z_Score_Original", "Z_Score_Prime").show(truncate=False)
        
        # Write to Silver Storage
        good_df.write.mode("append").parquet(SILVER_STORAGE_PATH)
        
        # Read Full Historical Dataset for Gold Layer Dashboards
        spark_session = good_df.sparkSession
        history_df = spark_session.read.parquet(SILVER_STORAGE_PATH)
        history_df.createOrReplaceTempView("historical_scores")

        # 1. Market Averages
        print(f"\n📊 [BATCH {batch_id}] GLOBAL MARKET AVERAGES BY YEAR")
        spark_session.sql("""
            SELECT year,
                   COUNT(ticker) as total_companies_analyzed,
                   ROUND(AVG(Z_Score_Prime), 2) as market_avg_z_prime,
                   ROUND(AVG(Z_Score_Original), 2) as market_avg_z_original
            FROM historical_scores
            GROUP BY year
            ORDER BY year DESC
        """).show(truncate=False)

        # 2. Leaderboards
        print(f"\n🏆 [BATCH {batch_id}] TOP-5 LEADERBOARD (By Modified Z'-Score)")
        spark_session.sql("""
            WITH RankedScores AS (
                SELECT year, ticker, Z_Score_Prime, Health_Zone_Prime,
                       RANK() OVER (PARTITION BY year ORDER BY Z_Score_Prime DESC) as rank
                FROM historical_scores
            )
            SELECT year, rank, ticker, Z_Score_Prime, Health_Zone_Prime
            FROM RankedScores
            WHERE rank <= 5
            ORDER BY year DESC, rank ASC
        """).show(n=50, truncate=False)
        
        log_message(f"[Batch {batch_id}] Dashboards successfully recalculated.", "INFO")

if __name__ == "__main__":
    try:
        log_message(f"Initializing {APP_NAME}...", "INFO")
        clean_silver_storage()

        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .master("local[*]") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")

        # Read Stream (Using 'latest' for Live Demo)
        raw_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), get_schema()).alias("data")
        ).select("data.*")
        
        # Execute Pipeline
        validated_stream = data_quality_gate(parsed_stream)
        clean_stream = preprocess_and_engineer_features(validated_stream)
        scored_stream = calculate_dual_z_scores(clean_stream)

        log_message("Starting continuous stream and dashboard engine...", "INFO")

        # Start Stream and execute micro-batches
        query = scored_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(process_micro_batch) \
            .start()
        
        query.awaitTermination()

    except Exception as e:
        log_message(f"Fatal error: {str(e)}", "ERROR")
        raise e