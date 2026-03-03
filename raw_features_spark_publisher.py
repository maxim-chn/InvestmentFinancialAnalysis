"""Goal: examine original 10-K reports in assets/filings_10k and publish .parquet
objects to Kafka as RAW_FEATURE DataFrames (see constants.py).
"""

from __future__ import annotations

import os
import re

from typing import List, Optional, Tuple

from pyspark.sql import SparkSession

from src.raw_features.combined_metrics import combine_metrics
from src.raw_features.consolidated_balance_sheet import (
  extract_metrics as extract_balance_sheet_metrics,
  read_raw_balance_sheet
)
from src.raw_features.consolidated_cashflow_statements import (
  extract_metrics as extract_cashflow_metrics,
  read_raw_cashflow_statements
)
from src.raw_features.constants import (
  BALANCE_SHEET_ERR_TEMPLATE,
  CASHFLOW_ERR_TEMPLATE,
  RAW_TABLES_DIR
)
from src.raw_features.logger import log_message

MAIN_PROCESS_NAME = "RawFeaturesSparkPublisher"
JOB_KEY_SEPARATOR = "@"
FISCAL_YEAR_THRESHOLD = "2015"

def get_fiscal_year_threshold() -> int:
  try:
    return int(FISCAL_YEAR_THRESHOLD)
  except ValueError as e:
    raise ValueError(
      f"FISCAL_YEAR_THRESHOLD must be a 4-digit year, got '{FISCAL_YEAR_THRESHOLD}'"
    ) from e

def get_required_env_var(name: str) -> str:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    raise RuntimeError(f"Missing required env var {name}")
  return value.strip()

def setup_kafka_channel():
  kafka_host = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST")
  kafka_port_raw = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT")
  kafka_channel = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL")
  try:
    kafka_port = int(kafka_port_raw)
  except ValueError as e:
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT must be a valid integer port"
    ) from e
  try:
    from confluent_kafka import Producer
  except ImportError as e:
    raise RuntimeError(
      "Missing dependency confluent_kafka. Install it (e.g. pip install confluent-kafka)."
    ) from e
  producer = Producer({"bootstrap.servers": f"{kafka_host}:{kafka_port}"})
  return producer, kafka_channel

def extract_fiscal_year(filename: str) -> Optional[str]:
  match = re.match(r"^filing-(\d{4})-", filename)
  if not match:
    return None
  return match.group(1)

def build_job_key(company: str, fiscal_year: str) -> str:
  return f"{company}{JOB_KEY_SEPARATOR}{fiscal_year}"

def parse_job_key(job_key: str) -> Tuple[str, str]:
  parts = job_key.split(JOB_KEY_SEPARATOR, 1)
  if len(parts) != 2:
    return job_key, "unknown"
  return parts[0], parts[1]

def list_company_filings(base_dir: str, target_company: str) -> List[Tuple[str, str]]:
  fiscal_year_threshold = get_fiscal_year_threshold()
  company = target_company.lower().strip()
  company_dir = os.path.join(base_dir, company)
  filings: List[Tuple[str, str]] = []

  if not os.path.isdir(company_dir):
    return filings

  for filename in sorted(os.listdir(company_dir)):
    fiscal_year = extract_fiscal_year(filename)
    if fiscal_year is None:
      continue
    if int(fiscal_year) < fiscal_year_threshold:
      continue
    job_key = build_job_key(company, fiscal_year)
    filings.append((job_key, os.path.join(company_dir, filename)))

  return filings

def clean_raw_html_tables(output_dir: str) -> None:
  for root, _dirs, files in os.walk(output_dir):
    for filename in files:
      if filename.endswith(".html"):
        try:
          os.remove(os.path.join(root, filename))
        except OSError:
          continue

def list_all_company_filings(base_dir: str) -> List[Tuple[str, str]]:
  filings: List[Tuple[str, str]] = []
  for dirname in sorted(os.listdir(base_dir)):
    company_dir = os.path.join(base_dir, dirname)
    if not os.path.isdir(company_dir):
      continue
    filings.extend(list_company_filings(base_dir, dirname))
  return filings

def main() -> None:
  fiscal_year_threshold = get_fiscal_year_threshold()
  assets_dir = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  if not os.path.isabs(assets_dir):
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_ASSETS must be an absolute path"
    )
  if not os.path.isdir(assets_dir):
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_ASSETS does not exist or is not a directory: "
      f"{assets_dir}"
    )

  target_company = os.getenv("RAW_FEATURES_SPARK_PUBLISHER_TARGET_COMPANY", "").strip()
  kafka_producer, kafka_channel = setup_kafka_channel()
  if target_company:
    log_message(
      "Single target company mode enabled; processing company '%s' only" % (
        target_company.upper()
      )
    )
    filings = list_company_filings(assets_dir, target_company)
  else:
    log_message("Full process mode enabled; processing all companies under assets path")
    filings = list_all_company_filings(assets_dir)
  
  if not filings:
    if target_company:
      log_message(
        "Company %s -- no filings to process at/after fiscal year threshold %s" % (
          target_company.upper(),
          str(fiscal_year_threshold)
        ),
        "ERROR"
      )
    else:
      log_message(
        "No filings found under %s at/after fiscal year threshold %s" % (
          assets_dir,
          str(fiscal_year_threshold)
        ),
        "ERROR"
      )
    return

  spark = SparkSession.builder.appName(MAIN_PROCESS_NAME).getOrCreate()
  sc = spark.sparkContext
  log_message(f"SparkSession started for {MAIN_PROCESS_NAME}")
  num_slices = max(1, min(len(filings), sc.defaultParallelism or len(filings)))
  if target_company:
    log_message(
      "Will process %s 10-K filings for company %s with %s spark workers "
      "(fiscal year threshold: %s)" % (
        str(len(filings)),
        target_company.upper(),
        str(num_slices),
        str(fiscal_year_threshold)
      )
    )
  else:
    log_message(
      "Will process %s 10-K filings across all companies with %s spark workers "
      "(fiscal year threshold: %s, assets path: %s)" % (
        str(len(filings)),
        str(num_slices),
        str(fiscal_year_threshold),
        assets_dir
      )
    )
  filings_rdd = sc.parallelize(filings, num_slices)
  balance_sheet = filings_rdd.mapPartitions(read_raw_balance_sheet).collect()
  abort_execution = False
  
  for filing_key, worker_output in balance_sheet:
    if BALANCE_SHEET_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to read Consolidated Balance Sheet -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      abort_execution = True

  if abort_execution:
    return
  
  log_message(f"Read {len(balance_sheet)} Consolidated Balance Sheet in total")
  balance_sheet_by_company = {filing_key: table for filing_key, table in balance_sheet}
  rdd = sc.parallelize(balance_sheet, num_slices)
  balance_sheet_metrics = rdd.mapPartitions(extract_balance_sheet_metrics).collect()

  for filing_key, worker_output in balance_sheet_metrics:
    if BALANCE_SHEET_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to analyze Consolidated Balance Sheet -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      log_message(f"Balance Sheet: {balance_sheet_by_company.get(filing_key)}")
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Analyzed {len(balance_sheet_metrics)} Consolidated Balance Sheet in total")
  filings_rdd = sc.parallelize(filings, num_slices)
  cashflow_statements = filings_rdd.mapPartitions(read_raw_cashflow_statements).collect()
  abort_execution = False

  for filing_key, worker_output in cashflow_statements:
    if CASHFLOW_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to read Consolidated Cash Flow Statements -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Read {len(cashflow_statements)} Consolidated Cash Flow Statements in total")
  cashflow_statements_by_company = {filing_key: table for filing_key, table in cashflow_statements}
  cashflow_rdd = sc.parallelize(cashflow_statements, num_slices)
  cashflow_metrics = cashflow_rdd.mapPartitions(extract_cashflow_metrics).collect()

  for filing_key, worker_output in cashflow_metrics:
    if CASHFLOW_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to analyze Consolidated Cash Flow Statements -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      log_message(f"Cash Flow Statements: {cashflow_statements_by_company.get(filing_key)}")
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Analyzed {len(cashflow_metrics)} Consolidated Cash Flow Statements in total")
  try:
    combined_metrics = combine_metrics(
      balance_sheet_metrics=balance_sheet_metrics,
      cashflow_metrics=cashflow_metrics,
      fiscal_year_threshold=fiscal_year_threshold,
      job_key_separator=JOB_KEY_SEPARATOR
    )
  except ValueError as e:
    log_message(str(e), "ERROR")
    return

  for company, serialized_metrics in combined_metrics:
    kafka_producer.produce(kafka_channel, key=company, value=serialized_metrics)
    kafka_producer.poll(0)
    log_message(
      "Published metrics for company '%s' to kafka channel '%s'" % (
        company.upper(),
        kafka_channel
      )
    )

  kafka_producer.flush()
  log_message(combined_metrics, "DEBUG")
  
  spark.stop()

if __name__ == "__main__":
  main()
