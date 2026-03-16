from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, when, lit, round, coalesce, avg, stddev, abs
from src.raw_features.logger import log_message

def enrich_with_analytics(df: DataFrame) -> DataFrame:
    """
    Advanced Analytics Step:
    Calculates dynamic sector averages, standard deviations, and assigns Health Zones.
    """
    stats_window = Window.partitionBy()

    # 1. Calculate Statistics (Avg & StdDev) per Year
    df_stats = df \
        .withColumn("Yearly_Avg_Z", round(avg("Z_Score").over(stats_window), 2)) \
        .withColumn("Yearly_StdDev", round(stddev("Z_Score").over(stats_window), 2)) \
        .na.fill(0.0, ["Yearly_StdDev"])

    # 2. Apply Logic: Benchmarking, Anomaly Detection, and Classification
    return df_stats.withColumn(
        "performance",
        when(col("Z_Score") > col("Yearly_Avg_Z"), "Outperforming")
        .otherwise("Underperforming")
    ).withColumn(
        "is_anomaly",
        when(abs(col("Z_Score") - col("Yearly_Avg_Z")) > (lit(2) * col("Yearly_StdDev")), "Yes")
        .otherwise("No")
    )

# --- CONFIGURATION CONSTANTS ---
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "raw_features"  # Update this to match your actual Kafka topic

def process_batch(df: DataFrame, batch_id: int):
    """
    Processes each micro-batch to log NaN sources and write to console.
    """
    # Log NaN sources
    nan_df = df.filter(F.isnan(col("Z_Score")) | col("Z_Score").isNull())
    for row in nan_df.select("ticker", "year", "X1", "X2", "X3", "X4", "X5").collect():
        log_message(
            f"NaN Z_Score for ticker: {row['ticker']}, year: {row['year']}. "
            f"X1={row['X1']}, X2={row['X2']}, X3={row['X3']}, X4={row['X4']}, X5={row['X5']}",
            log_name="AltmanZPrimeETL",
            level="WARNING"
        )

    # Clean the data by removing rows with NaN or null Z_Score
    cleaned_df = df.filter(~(F.isnan(col("Z_Score")) | col("Z_Score").isNull()))

    # Enrich with analytics on cleaned data
    enriched_df = enrich_with_analytics(cleaned_df)

    # Prepare and show output
    enriched_df.select(
        col("ticker").alias("Company"),
        "year",
        "Z_Score",
        "Health_Zone",
        "performance",
        "is_anomaly"
    ).withColumn("Company", F.upper(col("Company"))).show(truncate=False)
def get_schema() -> StructType:
    """
    Defines the schema for the incoming raw JSON data from the parser.
    Includes the 'units' field as a MapType to handle dynamic unit scaling.
    """
    return StructType([
        StructField("ticker", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("current_assets", FloatType(), True),
        StructField("current_liabilities", FloatType(), True),
        StructField("total_assets", FloatType(), True),
        StructField("stockholders_equity", FloatType(), True),
        StructField("short_term_debt", FloatType(), True),
        StructField("long_term_debt", FloatType(), True),
        StructField("net_income", FloatType(), True),
        StructField("interest_expense", FloatType(), True),
        StructField("tax_expense", FloatType(), True),
        StructField("retained_earnings", FloatType(), True),
        StructField("total_revenue", FloatType(), True),
        StructField("units", MapType(StringType(), StringType()), True)
    ])

def normalize_and_engineer_features(df: DataFrame) -> DataFrame:
    """
    Normalizes financial values to absolute dollars based on the 'units' field.
    Calculates missing intermediate metrics (Total Liabilities, EBIT).
    """
    # 1. Extract the unit string from the 'units' dictionary (using current_assets as a proxy)
    df = df.withColumn("unit_str", col("units").getItem("current_assets"))

    # 2. Define the multiplier based on the parsed unit string
    # Default to millions (1,000,000) if units are missing, as it's standard for US 10-K filings
    df = df.withColumn(
        "multiplier",
        when(col("unit_str").rlike("(?i)millions"), lit(1_000_000.0))
        .when(col("unit_str").rlike("(?i)thousands"), lit(1_000.0))
        .otherwise(lit(1_000_000.0)) 
    )

    # 3. Apply the multiplier to all monetary fields and handle NULL values (convert to 0.0)
    financial_fields = [
        "current_assets", "current_liabilities", "total_assets", "stockholders_equity",
        "short_term_debt", "long_term_debt", "net_income", "interest_expense", 
        "tax_expense", "retained_earnings", "total_revenue"
    ]
    
    for field in financial_fields:
        df = df.withColumn(field, coalesce(col(field), lit(0.0)) * col("multiplier"))

    # 4. Feature Engineering: Calculate necessary fields for the Altman formula
    # Total Liabilities = Current Liabilities + Long Term Debt
    df = df.withColumn("total_liabilities", col("current_liabilities") + col("long_term_debt"))
    
    # EBIT = Net Income + Interest Expense + Tax Expense
    df = df.withColumn("ebit", col("net_income") + col("interest_expense") + col("tax_expense"))
    
    # Drop invalid rows to prevent Division by Zero errors
    return df.filter(col("total_assets") > 0)

def calculate_altman_zprime(df: DataFrame) -> DataFrame:
    """
    Calculates the Altman Z'-Score (1983 revision for private/non-market companies).
    Uses Book Value of Equity instead of Market Capitalization.
    """
    # Calculate the 5 financial ratios
    df = df.withColumn("X1", (col("current_assets") - col("current_liabilities")) / col("total_assets")) \
           .withColumn("X2", col("retained_earnings") / col("total_assets")) \
           .withColumn("X3", col("ebit") / col("total_assets")) \
           .withColumn("X4", when(col("total_liabilities") != 0, col("stockholders_equity") / col("total_liabilities")).otherwise(None)) \
           .withColumn("X5", col("total_revenue") / col("total_assets"))

    # Apply the Z'-Score formula with adjusted weights
    # Z' = 0.717(X1) + 0.847(X2) + 3.107(X3) + 0.420(X4) + 0.998(X5)
    df = df.withColumn(
        "Z_Score", 
        round(
            (lit(0.717) * col("X1")) + 
            (lit(0.847) * col("X2")) + 
            (lit(3.107) * col("X3")) + 
            (lit(0.420) * col("X4")) + 
            (lit(0.998) * col("X5")), 
            2
        )
    )
    
    # Assign Risk Zones based on Z'-Score thresholds
    return df.withColumn(
        "Health_Zone",
        when(col("Z_Score") >= 2.90, "Safe (Green)")
        .when((col("Z_Score") >= 1.23) & (col("Z_Score") < 2.90), "Grey (Caution)")
        .otherwise("Distress (Red)")
    )

if __name__ == "__main__":
    print("Starting Altman Z'-Score Stream Processor...")

    # IMPORTANT: Set the root directory for the logger to work.
    # You can uncomment and set the path here, or set it as an environment variable.
    # import os
    # os.environ['RAW_FEATURES_SPARK_PUBLISHER_ROOT'] = '/home/linuxu/Dima/InvestmentFinancialAnalysis'

    spark = SparkSession.builder \
        .appName("Altman_Z_Prime_ETL") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read raw streaming data from Kafka
    raw_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), get_schema()).alias("data")
    ).select("data.*")
    
    # 2. Clean, normalize, and calculate
    clean_stream = normalize_and_engineer_features(parsed_stream)
    scored_stream = calculate_altman_zprime(clean_stream)

    # 3. Process each batch to log NaNs and write to console
    query = scored_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()