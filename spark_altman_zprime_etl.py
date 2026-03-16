from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, coalesce
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType

# --- CONFIGURATION CONSTANTS ---
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "raw_features"  # Update this to match your actual Kafka topic

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
           .withColumn("X4", col("stockholders_equity") / col("total_liabilities")) \
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

    # 3. Output results to console
    # Using 'append' mode since we are processing raw events, not aggregating yet
    query = scored_stream.select("ticker", "year", "Z_Score", "Health_Zone", "ebit", "total_liabilities") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    query.awaitTermination()