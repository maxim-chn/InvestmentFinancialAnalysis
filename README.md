# Real-Time Financial Health Analysis & Leaderboard

==============================================================================
DESCRIPTION
==============================================================================
This project implements a distributed Big Data system for assessing the financial 
health of companies using the **Altman Z-Score** model. 

The system simulates a real-time stream of financial reports (10-K), calculates 
Z-Scores on-the-fly, benchmarks companies against the market, and maintains a 
**Real-Time Top 5 Leaderboard** of the healthiest companies per year.

**Key Features:**
1. **Deterministic Financial Modeling:** Implementation of the Altman Z-Score formula (1968).
2. **Dynamic Benchmarking:** Comparing individual performance vs. real-time sector averages.
3. **Anomaly Detection:** Identifying statistical outliers (>2σ from the mean).
4. **Live Leaderboard:** A constantly updating "Top 5" list using Spark Window Functions.

==============================================================================
ARCHITECTURE & ROLES
==============================================================================
* **Student 1 (ETL):** Collects unstructured PDF/HTML reports, parses them using Spark Batch, 
  and persists clean data to CSV.
* **Student 2 (Streaming):** Implements the Kafka Producer and the Spark Structured 
  Streaming job (Z-Score calculation, Ranking, and Analytics).
* **Student 3 (Research):** Methodology selection and Literature Review.

==============================================================================
THE ALTMAN Z-SCORE MODEL
==============================================================================
**Formula:** `Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)`

**Variables:**
* **X1:** Working Capital / Total Assets
* **X2:** Retained Earnings / Total Assets
* **X3:** EBIT / Total Assets
* **X4:** Market Value of Equity / Total Liabilities
* **X5:** Sales / Total Assets

**Zones:**
* 🟢 **Safe:** Z > 2.99
* 🟡 **Grey:** 1.81 < Z < 2.99
* 🔴 **Distress:** Z < 1.81

==============================================================================
RAW_FEATURES EXTRACTOR (10-K HTML -> KAFKA JSON)
==============================================================================
`raw_features_spark_publisher.py` is the extraction job that converts raw 10-K HTML
files into normalized financial metrics and publishes them to Kafka.

**What it does:**
* Reads filings from `assets/filings_10k/<ticker>/filing-YYYY-*.htm(l)`.
* Extracts key metrics from Consolidated Balance Sheet and Consolidated Cash Flow tables:
  `common_stock_units`, `current_assets`, `current_liabilities`, `short_term_debt`,
  `long_term_debt`, `stockholders_equity`, `total_assets`, `net_income`,
  `interest_expense`, `tax_expense`.
* Merges filing-level metrics into one consolidated payload per company (from fiscal year 2015+).
* Publishes one JSON message per company to the configured Kafka topic.

Operate the extractor through the `Makefile` component target:

```bash
make raw_features process
make raw_features process company=aapl
make raw_features do_export
make raw_features do_import
```

Use `make help` for configurable variables (`RAW_FEATURES_SPARK_PUBLISHER_*`) and defaults.
Runtime logs are written to `logs/raw_features_spark_publisher.log`.

**How it achieves this (implementation details):**
The job enumerates company filings by filename (`filing-YYYY-*`) and filters by fiscal
year threshold. It parallelizes file processing with Spark RDD partitions, then applies
table-selection heuristics (keyword markers, exclusions, and year-density scoring) to
choose the best Balance Sheet and Cash Flow tables from each HTML filing. Metric extractors
in `src/raw_features/*_rules.py` parse values by year and validate required fields.
Finally, `combine_metrics` merges all filing-level frames per company, keeps the most useful
non-fallback values, preserves units in column labels, and serializes one JSON payload that
is produced to Kafka with the company ticker as message key.

==============================================================================
EXECUTION INSTRUCTIONS
==============================================================================

STEP 0: CONFIGURE AND START KAFKA
---------------------------------
1.  Navigate to Kafka directory:
    cd /usr/local/kafka/kafka_2.13-3.2.1

2.  Fix Configuration (One-time setup):
    sed -i 's|#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://:9092|' config/server.properties

3.  Start Zookeeper (Terminal 1):
    bin/zookeeper-server-start.sh config/zookeeper.properties

4.  Start Kafka Server (Terminal 2):
    bin/kafka-server-start.sh config/server.properties

5.  Create Topic (Terminal 3):
    bin/kafka-topics.sh --create --topic financial_reports_stream --bootstrap-server localhost:9092

------------------------------------------------------------------------------

STEP 1: RUN DATA PRODUCER (TERMINAL 3)
--------------------------------------
Simulates real-time report ingestion from market API data.

1.  Navigate to project folder:
    cd /path/to/project

2.  Run producer:
    python3 produser.py

------------------------------------------------------------------------------

STEP 2: RUN SPARK ANALYTICS ENGINE (TERMINAL 4)
-----------------------------------------------
This job computes the Z-Scores and maintains the Top 5 Leaderboard.
**Note:** This job runs in 'Complete' mode to handle dynamic re-ranking of companies.

1.  Submit the Spark job:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_altman_etl.py

    > **Expected Output:**
    > +----+----+------+-------+-------------+---------------+----------+
    > |year|Rank|ticker|Z_Score|Health_Zone  |Performance    |Is_Anomaly|
    > +----+----+------+-------+-------------+---------------+----------+
    > |2023|1   |NVDA  |15.40  |Safe (Green) |Outperforming  |YES       |
    > |2023|2   |MSFT  |8.20   |Safe (Green) |Outperforming  |No        |
    > |2023|3   |AAPL  |7.55   |Safe (Green) |Outperforming  |No        |
    > ...
    > +----+----+------+-------+-------------+---------------+----------+

==============================================================================
TROUBLESHOOTING
==============================================================================
* **Why 'Complete' Mode?** We use `outputMode("complete")` because ranking (Top 5) is a relative calculation. 
  When a new high-performing company arrives, it shifts the rank of all others, 
  requiring a refresh of the entire leaderboard table.
