RAW_FEATURES_SPARK_PUBLISHER_ROOT ?= $(CURDIR)
RAW_FEATURES_SPARK_PUBLISHER_ASSETS ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets/filings_10k
RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets_export.zip
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST ?= localhost
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT ?= 9092
RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL ?= raw_features
PYTHON ?= /home/linuxu/anaconda3/bin/python
company ?=
assets_dir := $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)
publisher_script := $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/raw_features_spark_publisher.py
supported_component := raw_features
selected_component := $(firstword $(MAKECMDGOALS))

.DEFAULT_GOAL := help

.PHONY: help raw_features process do_export do_import run-altman-etl run-altman-zprime-etl run-producer

help:
	@echo "Usage: make <target> [options]"
	@echo ""
	@echo "Targets:"
	@echo "  raw_features            Supported component. See actions below."
	@echo "  run-altman-etl          Run spark_altman_etl.py."
	@echo "  run-altman-zprime-etl   Run spark_altman_zprime_etl.py."
	@echo "  run-producer            Run produser.py."
	@echo ""
	@echo "Actions for 'raw_features':"
	@echo "  process                 Run raw_features_spark_publisher.py."
	@echo "                          Optional: company=<ticker> (single-company mode)."
	@echo "  do_export               Zip assets dir to assets export archive."
	@echo "  do_import               Import assets archive unless assets dir is non-empty."
	@echo ""
	@echo "Options (override with make VAR=value):"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ROOT                  default: $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS                default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT         default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL  default: $(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)"
	@echo "  PYTHON                                             default: $(PYTHON)"
	@echo ""
	@echo "Examples:"
	@echo "  make raw_features process"
	@echo "  make raw_features process company=aaoi"
	@echo "  make raw_features do_export"
	@echo "  make raw_features do_import"
	@echo "  make run-altman-etl"
	@echo "  make run-altman-zprime-etl"
	@echo "  make run-producer"

raw_features:
	@if [ "$(selected_component)" != "$(supported_component)" ]; then \
		echo "ERROR: unsupported component '$(selected_component)'. Supported component: $(supported_component)"; \
		exit 1; \
	fi
	@if [ "$(words $(MAKECMDGOALS))" -eq 1 ]; then \
		echo "INFO: component '$(supported_component)' selected. Choose one action: process | do_export | do_import"; \
	fi

process: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(publisher_script)" ]; then \
		echo "ERROR: publisher script not found: $(publisher_script)"; \
		exit 1; \
	fi
	@if [ -n "$(company)" ]; then \
		echo "INFO: process mode single company=$(company)"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_COMPANY="$(company)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	else \
		echo "INFO: process mode full assets scan"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	fi

do_export: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -d "$(assets_dir)" ]; then \
		echo "ERROR: assets dir not found: $(assets_dir)"; \
		exit 1; \
	fi
	@mkdir -p "$(dir $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT))"
	@rm -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@cd "$(assets_dir)" && zip -rq "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" .
	@echo "INFO: assets exported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"

do_import: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" ]; then \
		echo "ERROR: export zip not found: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"; \
		exit 1; \
	fi
	@if [ -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" ] && [ "$$(ls -A "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" 2>/dev/null)" ]; then \
		echo "WARN: assets import is skipped because assets already exist: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
	else \
		if [ ! -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" ]; then \
			mkdir -p "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
		fi; \
		unzip -qo "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
		echo "INFO: assets imported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
	fi

run-altman-etl:
	RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_altman_etl.py

run-altman-zprime-etl:
	RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_altman_zprime_etl.py

run-producer:
	python3 produser.py

%:
	@echo "ERROR: unsupported component '$@'. Supported component: $(supported_component)"
	@exit 1
\n\n---\n\n
# src/raw_features/price.py
from __future__ import annotations

import json
import os
import threading

from typing import Dict, Optional, Union

from src.raw_features.logger import log_message

PriceValue = Union[float, str]

_PRICE_CACHE: Dict[str, Dict[str, PriceValue]] = {}
_PRICE_CACHE_LOADED = False
_PRICE_CACHE_LOCK = threading.Lock()
_PRICE_CACHE_PATH_LOGGED = False


def _log_price_issue(message: str, level: str = "INFO") -> None:
  try:
    log_message(message, level=level)
  except Exception:
    return


def _log_price_info(message: str) -> None:
  _log_price_issue(message, level="INFO")


def _get_assets_dir() -> str:
  assets_dir = os.getenv("RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  if assets_dir is None or assets_dir.strip() == "":
    raise RuntimeError("Missing required env var RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  return assets_dir.strip()


def _get_price_cache_path() -> str:
  return os.path.join(_get_assets_dir(), "price_cache.json")


def _normalize_ticker(ticker: object) -> str:
  return str(ticker).strip().upper()


def _normalize_year_key(year: object) -> str:
  return str(int(year))


def _normalize_price_value(price: PriceValue) -> PriceValue:
  if isinstance(price, (int, float)) and not isinstance(price, bool):
    return round(float(price), 4)
  return price


def _load_price_cache() -> Dict[str, Dict[str, PriceValue]]:
  global _PRICE_CACHE, _PRICE_CACHE_LOADED, _PRICE_CACHE_PATH_LOGGED

  if _PRICE_CACHE_LOADED:
    return _PRICE_CACHE

  with _PRICE_CACHE_LOCK:
    if _PRICE_CACHE_LOADED:
      return _PRICE_CACHE

    cache_path = _get_price_cache_path()
    if not _PRICE_CACHE_PATH_LOGGED:
      _log_price_info(f"Price cache path: {cache_path}")
      _PRICE_CACHE_PATH_LOGGED = True
    try:
      with open(cache_path, "r", encoding="utf-8") as cache_file:
        raw_cache = json.load(cache_file)
    except FileNotFoundError:
      raw_cache = {}
    except json.JSONDecodeError:
      raw_cache = {}

    normalized_cache: Dict[str, Dict[str, PriceValue]] = {}
    if isinstance(raw_cache, dict):
      for raw_ticker, ticker_prices in raw_cache.items():
        if not isinstance(ticker_prices, dict):
          continue
        ticker_key = _normalize_ticker(raw_ticker)
        normalized_cache[ticker_key] = {}
        for raw_year, price in ticker_prices.items():
          try:
            year_key = _normalize_year_key(raw_year)
          except (TypeError, ValueError):
            continue
          normalized_cache[ticker_key][year_key] = _normalize_price_value(price)

    _PRICE_CACHE = normalized_cache
    _PRICE_CACHE_LOADED = True
    return _PRICE_CACHE


def get_price_from_cache(ticker: str, year: int) -> Optional[PriceValue]:
  cache = _load_price_cache()
  ticker_key = _normalize_ticker(ticker)
  try:
    year_key = _normalize_year_key(year)
  except (TypeError, ValueError):
    return None
  return cache.get(ticker_key, {}).get(year_key)


def update_price_in_cache(ticker: str, year: int, price: PriceValue) -> PriceValue:
  cache = _load_price_cache()
  ticker_key = _normalize_ticker(ticker)
  year_key = _normalize_year_key(year)
  normalized_price = _normalize_price_value(price)

  with _PRICE_CACHE_LOCK:
    ticker_cache = cache.setdefault(ticker_key, {})
    ticker_cache[year_key] = normalized_price

    cache_path = _get_price_cache_path()
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as cache_file:
      json.dump(cache, cache_file, indent=2, sort_keys=True)
      cache_file.write("\n")
    _log_price_info(
      "Stored price for ticker '%s' fiscal year %s in cache file '%s': %s" % (
        ticker_key,
        year_key,
        cache_path,
        str(normalized_price)
      )
    )

  return normalized_price


def _extract_close_price_from_history(history) -> Optional[float]:
  if history is None or history.empty or "Close" not in history:
    return None

  close_values = history["Close"].dropna()
  if close_values.empty:
    return None

  try:
    return float(close_values.iloc[-1])
  except (TypeError, ValueError):
    return None


def _get_price_from_yfinance(ticker: str, year: int) -> Optional[float]:
  try:
    import yfinance as yf
  except ImportError:
    _log_price_issue(
      "Price lookup skipped for ticker '%s' fiscal year %s -- missing dependency yfinance" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="ERROR"
    )
    return None

  start = f"{year}-01-01"
  end = f"{year + 1}-01-01"

  try:
    ticker_handle = yf.Ticker(_normalize_ticker(ticker))
    history = ticker_handle.history(
      start=start,
      end=end,
      auto_adjust=False
    )
  except Exception as e:
    _log_price_issue(
      "Price lookup failed for ticker '%s' fiscal year %s -- %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(e)
      ),
      level="ERROR"
    )
    return None

  price = _extract_close_price_from_history(history)
  if price is not None:
    price = _normalize_price_value(price)
    _log_price_info(
      "Fetched price from Yahoo for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(price)
      )
    )
    return price

  try:
    max_history = ticker_handle.history(period="max", auto_adjust=False)
  except Exception as e:
    _log_price_issue(
      "Price history fallback failed for ticker '%s' fiscal year %s -- %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(e)
      ),
      level="ERROR"
    )
    return None

  try:
    yearly_history = max_history[max_history.index.year == year]
  except Exception:
    yearly_history = None

  price = _extract_close_price_from_history(yearly_history)
  if price is not None:
    price = _normalize_price_value(price)
    _log_price_info(
      "Fetched price from Yahoo fallback history for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(price)
      )
    )
    return price

  _log_price_issue(
    "Price lookup returned no close history for ticker '%s' fiscal year %s" % (
      _normalize_ticker(ticker),
      str(year)
    ),
    level="WARNING"
  )
  return None


def get_price(ticker: str, year: int) -> PriceValue:
  cached_price = get_price_from_cache(ticker, year)
  if cached_price is not None:
    cached_price = _normalize_price_value(cached_price)
    _log_price_info(
      "Loaded price from cache for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(cached_price)
      )
    )
    return cached_price

  try:
    normalized_year = int(year)
  except (TypeError, ValueError):
    _log_price_issue(
      "Price lookup skipped for ticker '%s' -- invalid fiscal year '%s'" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="ERROR"
    )
    normalized_year = -1

  price = None
  if normalized_year > 0:
    price = _get_price_from_yfinance(ticker, normalized_year)

  if price is None:
    price = "N/A"
    _log_price_issue(
      "Using fallback price N/A for ticker '%s' fiscal year %s" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="WARNING"
    )

  return update_price_in_cache(ticker, year, price)
\n\n---\n\n
# src/raw_features/original_filing.py
import os

from typing import Any, Callable, List, Optional, Tuple

from src.raw_features.constants import TARGET_YEAR_PREFIX_TEMPLATE

def extract_tables_from_html(html_text: str, predicate: Callable[[Any], bool]) -> List[str]:
  from bs4 import BeautifulSoup
  from bs4.element import Tag

  soup = BeautifulSoup(html_text, "html.parser")
  tables: List[str] = []

  def _is_context_node(node: Tag) -> bool:
    if node.name not in ("p", "div"):
      return False
    if node.find("table") is not None:
      return False
    text = node.get_text(" ", strip=True)
    return bool(text) and len(text) <= 200

  def _collect_previous_context(node: Tag, limit: int) -> List[Tag]:
    collected: List[Tag] = []
    for sibling in node.previous_siblings:
      if len(collected) >= limit:
        break
      if not isinstance(sibling, Tag):
        continue
      if sibling.name == "table":
        break
      if _is_context_node(sibling):
        collected.append(sibling)
    collected.reverse()
    return collected
  
  for table in soup.find_all("table"):
    
    if not predicate(table):
      continue

    context_nodes: List[Tag] = []
    max_context_nodes = 4
    parent = table.parent if isinstance(table.parent, Tag) else None
    if parent is not None:
      context_nodes.extend(_collect_previous_context(parent, max_context_nodes))
    if len(context_nodes) < max_context_nodes:
      context_nodes.extend(_collect_previous_context(table, max_context_nodes - len(context_nodes)))
    context_html = "".join(str(node) for node in context_nodes) + str(table)
    tables.append(context_html)
  
  return tables

def list_filings(base_dir: str, target_fiscal_year: str) -> List[Tuple[str, str]]:
  filings: List[Tuple[str, str]] = []
  
  for company in sorted(os.listdir(base_dir)):
    company_dir = os.path.join(base_dir, company)
    
    if not os.path.isdir(company_dir):
      continue
    
    for filename in sorted(os.listdir(company_dir)):
      if not filename.startswith(TARGET_YEAR_PREFIX_TEMPLATE % target_fiscal_year):
        continue
      
      filings.append((company, os.path.join(company_dir, filename)))

  return filings

def read_original_filing(company: str, path: str) -> Optional[Tuple[str, str, str]]:
  try:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
      html_text = f.read()
  except OSError:
    return None
  return company, os.path.basename(path), html_text
\n\n---\n\n
# src/raw_features/logger.py
import os
from datetime import datetime

def get_required_env_var(name: str) -> str:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    raise RuntimeError(f"Missing required env var {name}")
  return value.strip()

BASE_DIR = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_ROOT")
LOGS_DIR = os.path.normpath(os.path.join(BASE_DIR, "logs"))
LOG_PATH = os.path.join(LOGS_DIR, "main.log")
_LOG_INITIALIZED = False


def _initialize_log() -> None:
  global _LOG_INITIALIZED
  if _LOG_INITIALIZED:
    return
  os.makedirs(LOGS_DIR, exist_ok=True)
  with open(LOG_PATH, "w", encoding="utf-8"):
    pass
  _LOG_INITIALIZED = True


def log_message(message: str, log_name: str = "RawFeaturesSparkPublisher", level: str = "INFO") -> None:
  _initialize_log()
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  line = f"{timestamp} -- {log_name} -- {level} -- {message}\n"
  with open(LOG_PATH, "a", encoding="utf-8") as f:
    f.write(line)
\n\n---\n\n
# src/raw_features/constants.py
import os
from enum import Enum

def get_required_env_var(name: str) -> str:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    raise RuntimeError(f"Missing required env var {name}")
  return value.strip()

BALANCE_SHEET_ERR_TEMPLATE = "failed to process balance sheet"
CASHFLOW_ERR_TEMPLATE = "failed to process cash flow statement"
BASE_DIR = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_ROOT")
FILINGS_DIR = os.path.join(BASE_DIR, "assets", "filings_10k")
RAW_TABLES_DIR = os.path.join(BASE_DIR, "assets", "raw_html_tables")
TARGET_YEAR_PREFIX_TEMPLATE = "filing-%s-"

class RAW_FEATURES(Enum):
  COMMON_STOCK_UNITS = "common_stock_units"
  CURRENT_ASSETS = "current_assets"
  CURRENT_LIABILITIES = "current_liabilities"
  LONG_TERM_DEBT = "long_term_debt"
  INTEREST_EXPENSE = "interest_expense"
  NET_INCOME = "net_income"
  RETAINED_EARNINGS = "retained_earnings"
  SHORT_TERM_DEBT = "short_term_debt"
  STOCKHOLDERS_EQUITY = "stockholders_equity"
  TAX_EXPENSE = "tax_expense"
  TOTAL_ASSETS = "total_assets"
  TOTAL_REVENUE = "total_revenue"
  UNITS = "units"
\n\n---\n\n
# src/raw_features/consolidated_cashflow_statements_rules.py
import re
from typing import Optional

from src.raw_features.constants import RAW_FEATURES

TOTAL_REVENUE_FACT_MARKERS = (
  'name="us-gaap:revenues"',
  'name="us-gaap:salesrevenuenet"',
  'name="us-gaap:revenuefromcontractwithcustomerexcludingassessedtax"',
  'name="us-gaap:revenuefromcontractwithcustomerincludingassessedtax"',
  'name="us-gaap:totalrevenuesandotherincome"',
  "name='us-gaap:revenues'",
  "name='us-gaap:salesrevenuenet'",
  "name='us-gaap:revenuefromcontractwithcustomerexcludingassessedtax'",
  "name='us-gaap:revenuefromcontractwithcustomerincludingassessedtax'",
  "name='us-gaap:totalrevenuesandotherincome'",
)

TOTAL_REVENUE_FACT_NAMES = (
  "us-gaap:revenues",
  "us-gaap:salesrevenuenet",
  "us-gaap:revenuefromcontractwithcustomerexcludingassessedtax",
  "us-gaap:revenuefromcontractwithcustomerincludingassessedtax",
  "us-gaap:totalrevenuesandotherincome",
)

MANDATORY_CASHFLOW_TERMS = [
  "investing activities",
  "financing activities",
  "operating activities",
]

CONDENSED_CASHFLOW_PATTERNS = (
  "condensed statements of cash flows",
  "condensed statement of cash flows",
  "condensed statements of cash flow",
  "condensed statement of cash flow",
)

def _has_consolidated_cashflow_heading(text: str) -> bool:
  return (
    "consolidated" in text
    and "cash flow" in text
    and "statement" in text
  )

def _context_text(table, limit: int = 20) -> str:
  texts: list[str] = []

  for node in table.find_all_previous(["p", "div"], limit=limit):
    if node.find("table") is not None:
      continue
    text = node.get_text(" ", strip=True)
    if text:
      texts.append(text)

  if not texts:
    return ""
  texts.reverse()
  return " ".join(texts)

def has_mandatory_metrics(table) -> bool:
  table_text = table.get_text(" ", strip=True).lower()
  context = _context_text(table).lower()
  combined = f"{context} {table_text}".strip()

  if any(pattern in combined for pattern in CONDENSED_CASHFLOW_PATTERNS):
    return False

  activities_present = sum(term in table_text for term in MANDATORY_CASHFLOW_TERMS)
  tr_count = len(table.find_all("tr"))
  if activities_present < 2 or tr_count < 12:
    return False

  if _has_consolidated_cashflow_heading(combined):
    return True

  return activities_present == 3 and tr_count >= 20

def _expand_row_cells(row) -> list[str]:
  expanded = []
  for cell in row.find_all(["td", "th"]):
    colspan = int(cell.get("colspan", 1))
    text = cell.get_text(" ", strip=True)
    for _ in range(colspan):
      expanded.append(text)
  return expanded


def _year_by_column(table) -> dict[int, str]:
  for row in table.find_all("tr"):
    cols = _expand_row_cells(row)
    year_hits = [re.search(r"\b(20\d{2})\b", text) for text in cols]
    if sum(1 for hit in year_hits if hit) >= 2:
      return {
        idx: hit.group(1)
        for idx, hit in enumerate(year_hits)
        if hit is not None
      }
  return {}

def _year_headers(table) -> list[str]:
  for row in table.find_all("tr"):
    years: list[str] = []
    for cell in row.find_all(["td", "th"]):
      for match in re.findall(r"\b(20\d{2})\b", cell.get_text(" ", strip=True)):
        if match not in years:
          years.append(match)
    if len(years) >= 2:
      return years
  return []

def extract_fiscal_years(table) -> list[str]:
  headers = _year_headers(table)
  if headers:
    return headers
  year_by_col = _year_by_column(table)
  if year_by_col:
    return sorted(set(year_by_col.values()))
  return []


def _parse_number(text: str) -> Optional[int]:
  match = re.search(r"\d[\d,]*", text)
  if not match:
    stripped = text.strip()
    if stripped and re.fullmatch(r"[—–-]+", stripped):
      return 0
    return None
  value = int(match.group(0).replace(",", ""))
  if "(" in text and ")" in text:
    return -value
  return value

def _cell_is_negative(cell, text: str) -> bool:
  if "(" in text and ")" in text:
    return True
  if re.search(r"-\s*\d", text):
    return True
  for tag in cell.find_all(True):
    name = (tag.name or "").lower()
    if name.endswith("nonfraction") and tag.get("sign") == "-":
      return True
  return False


def _is_net_income_row(row_text: str) -> bool:
  normalized = re.sub(r"\s+", " ", row_text).strip()
  if "per share" in row_text:
    return False
  if "noncontrolling" in row_text or "non-controlling" in row_text:
    return False
  if "attributable to" in row_text or "attributable" in row_text:
    return False
  if "minority interest" in row_text:
    return False
  if re.match(r"^net\s*\(\s*loss\s*\)\s*income\b", normalized):
    return True
  if re.match(r"^net\s*income\s*\(\s*loss\s*\)\b", normalized):
    return True
  if re.match(r"^net\s*earnings\s*\(\s*loss\s*\)\b", normalized):
    return True
  if "net income" in row_text or "net loss" in row_text:
    return True
  if "net earnings" in row_text:
    return True
  return False

def _is_interest_expense_row(row_text: str) -> bool:
  if "interest" not in row_text:
    return False
  if any(
    marker in row_text
    for marker in (
      "interest income",
      "interest earned",
      "cash paid for interest",
      "interest paid",
      "interest payments",
      "interest payment",
      "interest rate",
      "rate of interest",
      "non-interest",
    )
  ):
    return False
  if "net interest expense" in row_text:
    return True
  if "interest expense" in row_text:
    return True
  if "interest, net" in row_text and "income" not in row_text:
    return True
  return False

def _is_interest_paid_row(row_text: str) -> bool:
  if "interest" not in row_text:
    return False
  if "interest income" in row_text or "interest earned" in row_text:
    return False
  normalized = row_text.strip()
  if normalized in ("interest", "interest:"):
    return True
  if normalized.startswith("interest ") or normalized.startswith("interest: "):
    return True
  if "cash paid for interest" in row_text:
    return True
  if "cash paid during the year for interest" in row_text:
    return True
  if "cash payments for interest" in row_text:
    return True
  if "interest paid" in row_text:
    return True
  if "interest payments" in row_text or "interest payment" in row_text:
    return True
  return False

def _is_tax_expense_row(row_text: str) -> bool:
  if "tax" not in row_text:
    return False
  if any(
    marker in row_text
    for marker in (
      "cash paid for income taxes",
      "income taxes paid",
      "taxes paid",
      "tax paid",
      "tax refunds",
      "tax refund",
      "payroll tax",
      "tax rate",
      "effective tax",
      "tax withholding",
    )
  ):
    return False
  if "tax expense" in row_text:
    return True
  if "tax provision" in row_text:
    return True
  if "provision for income tax" in row_text or "provision for income taxes" in row_text:
    return True
  if "income tax provision" in row_text or "income taxes provision" in row_text:
    return True
  if "tax benefit" in row_text:
    return True
  return False

def _is_total_revenue_row(row_text: str) -> bool:
  normalized = re.sub(r"\s+", " ", row_text).strip()
  if not normalized:
    return False
  if "total revenues and other income" in normalized:
    return True
  if any(
    marker in normalized
    for marker in (
      "cost of revenue",
      "cost of revenues",
      "cost of sales",
      "cost of goods sold",
      "deferred revenue",
      "unearned revenue",
      "other revenue",
      "other revenues",
      "non-revenue",
      "segment revenue",
      "segment revenues",
      "per share",
      "percent of net sales",
      "percentage of net sales",
      "as a percent of net sales",
      "% of net sales",
    )
  ):
    return False
  if any(
    marker in normalized
    for marker in (
      "total revenue",
      "total revenues",
      "total net revenue",
      "total net revenues",
      "total net sales",
      "net revenue",
      "net revenues",
      "net sales",
      "revenue, net",
      "revenues, net",
      "sales revenue",
      "sales revenues",
    )
  ):
    return True
  if re.fullmatch(r"revenues?", normalized):
    return True
  return False

def _is_gross_profit_row(row_text: str) -> bool:
  if "gross profit" not in row_text:
    return False
  if any(
    marker in row_text
    for marker in (
      "gross profit margin",
      "percentage",
      "percent",
      "% of",
    )
  ):
    return False
  return True

def _is_cost_of_revenue_row(row_text: str) -> bool:
  if any(
    marker in row_text
    for marker in (
      "cost of revenue",
      "cost of revenues",
      "cost of goods sold",
      "cost of sales",
    )
  ):
    if any(
      marker in row_text
      for marker in (
        "percentage",
        "percent",
        "% of",
      )
    ):
      return False
    return True
  return False

def _is_tax_paid_row(row_text: str) -> bool:
  if "tax" not in row_text:
    return False
  normalized = row_text.strip()
  if any(marker in normalized for marker in ("accrued", "prepaid", "withholding")):
    return False
  if "cash paid for income taxes" in row_text:
    return True
  if "income taxes paid" in row_text:
    return True
  if normalized.startswith("income taxes"):
    return True
  if "taxes paid" in row_text and "payroll" not in row_text:
    return True
  if "tax paid" in row_text and "payroll" not in row_text:
    return True
  return False

def _is_deferred_tax_row(row_text: str) -> bool:
  if "deferred income tax" in row_text or "deferred income taxes" in row_text:
    return True
  if "deferred tax" in row_text:
    return not any(
      marker in row_text
      for marker in (
        "assets",
        "liabilities",
        "asset",
        "liability",
      )
    )
  return False


def _row_indicates_loss(row_text: str) -> bool:
  if "net loss" in row_text:
    return True
  return False


def extract_net_income(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _year_headers(table)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()
    if not _is_net_income_row(row_text):
      continue
    loss_row = _row_indicates_loss(row_text)
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        if _cell_is_negative(cell, cell_text):
          value = -abs(value)
        elif loss_row and value > 0:
          value = -value
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    return results
  return {}

def _extract_metric_from_rows(table, row_matcher) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _year_headers(table)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()
    if not row_matcher(row_text):
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        if _cell_is_negative(cell, cell_text):
          value = -abs(value)
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    # Some filings include heading rows (e.g., "Revenue, net") without
    # numeric cells before the actual data row. Keep scanning in that case.
    if results:
      return results
  return {}

def _extract_metric_from_fact_markers(table, fact_markers: tuple[str, ...]) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _year_headers(table)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_html = str(row).lower()
    if not any(marker in row_html for marker in fact_markers):
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        if _cell_is_negative(cell, cell_text):
          value = -abs(value)
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    if results:
      return results
  return {}

def _extract_attr(attrs: str, attr_name: str) -> Optional[str]:
  pattern = rf"""\b{re.escape(attr_name)}\s*=\s*(['"])(.*?)\1"""
  match = re.search(pattern, attrs, flags=re.IGNORECASE | re.DOTALL)
  if not match:
    return None
  return match.group(2)

def _extract_metric_from_inline_xbrl_context(
  table,
  fact_names: tuple[str, ...],
) -> dict[str, int]:
  table_html = str(table)
  fact_names_set = set(fact_names)
  matches = re.finditer(
    r"<ix:nonfraction\b(?P<attrs>[^>]*)>(?P<value>.*?)</ix:nonfraction>",
    table_html,
    flags=re.IGNORECASE | re.DOTALL,
  )

  results: dict[str, int] = {}
  for match in matches:
    attrs = match.group("attrs")
    fact_name = _extract_attr(attrs, "name")
    if fact_name is None or fact_name.lower() not in fact_names_set:
      continue

    contextref = _extract_attr(attrs, "contextref")
    if contextref is None:
      continue
    context_lower = contextref.lower()
    # Exclude dimensional segment/member rows; we need consolidated totals.
    if "axis" in context_lower or "member" in context_lower:
      continue

    year = None
    range_match = re.search(r"d(20\d{2})\d{4}-(20\d{2})\d{4}", contextref, flags=re.IGNORECASE)
    if range_match is not None:
      # For duration contexts, use the period end year.
      year = range_match.group(2)
    if year is None:
      year_match = re.search(r"(?:^|_)(20\d{2})[-_]", contextref)
      if year_match is None:
        year_match = re.search(r"\b(20\d{2})\b", contextref)
      if year_match is None:
        continue
      year = year_match.group(1)

    raw_value = re.sub(r"<[^>]+>", "", match.group("value"))
    value = _parse_number(raw_value)
    if value is None:
      continue

    sign = _extract_attr(attrs, "sign")
    if sign == "-":
      value = -abs(value)

    # Keep the larger absolute candidate if duplicates exist for the same year.
    if year in results and abs(results[year]) >= abs(value):
      continue
    results[year] = value

  return results

def extract_interest_expense(table) -> Optional[dict[str, int]]:
  interest_expense = _extract_metric_from_rows(table, _is_interest_expense_row)
  if interest_expense:
    return interest_expense

  interest_paid = _extract_metric_from_rows(table, _is_interest_paid_row)
  if interest_paid:
    return interest_paid

  return None

def extract_tax_expense(table) -> Optional[dict[str, int]]:
  tax_expense = _extract_metric_from_rows(table, _is_tax_expense_row)
  if tax_expense:
    return tax_expense

  tax_paid = _extract_metric_from_rows(table, _is_tax_paid_row)
  if tax_paid:
    return tax_paid

  deferred_tax = _extract_metric_from_rows(table, _is_deferred_tax_row)
  if deferred_tax:
    return deferred_tax

  return None

def _derive_total_revenue_from_components(table) -> Optional[dict[str, int]]:
  gross_profit = _extract_metric_from_rows(table, _is_gross_profit_row)
  if not gross_profit:
    return None
  cost_of_revenue = _extract_metric_from_rows(table, _is_cost_of_revenue_row)
  if not cost_of_revenue:
    return None

  derived_total_revenue: dict[str, int] = {}
  for year, gross_value in gross_profit.items():
    if year not in cost_of_revenue:
      continue
    derived_total_revenue[year] = gross_value + abs(cost_of_revenue[year])

  if not derived_total_revenue:
    return None
  return derived_total_revenue

def extract_total_revenue(table) -> Optional[dict[str, int]]:
  total_revenue = _extract_metric_from_fact_markers(table, TOTAL_REVENUE_FACT_MARKERS)
  if total_revenue:
    return total_revenue
  total_revenue = _extract_metric_from_inline_xbrl_context(table, TOTAL_REVENUE_FACT_NAMES)
  if total_revenue:
    return total_revenue
  total_revenue = _extract_metric_from_rows(table, _is_total_revenue_row)
  if total_revenue:
    return total_revenue
  total_revenue = _derive_total_revenue_from_components(table)
  if total_revenue:
    return total_revenue
  return None


CONSOLIDATED_BALANCE_SHEET_RULES = [has_mandatory_metrics]

METRIC_EXTRACT = {
  RAW_FEATURES.NET_INCOME.value: extract_net_income,
  RAW_FEATURES.INTEREST_EXPENSE.value: extract_interest_expense,
  RAW_FEATURES.TAX_EXPENSE.value: extract_tax_expense,
  RAW_FEATURES.TOTAL_REVENUE.value: extract_total_revenue,
}
\n\n---\n\n
# src/raw_features/consolidated_cashflow_statements.py
import re
from typing import Iterator, List, Optional, Tuple

from src.raw_features.consolidated_cashflow_statements_rules import (
  CONSOLIDATED_BALANCE_SHEET_RULES,
  METRIC_EXTRACT,
  extract_fiscal_years,
)
from src.raw_features.constants import CASHFLOW_ERR_TEMPLATE, RAW_FEATURES
from src.raw_features.original_filing import extract_tables_from_html, read_original_filing

INTEREST_PAID_FACT_MARKERS = (
  'name="us-gaap:interestpaid"',
  'name="us-gaap:interestpaidnet"',
  "name='us-gaap:interestpaid'",
  "name='us-gaap:interestpaidnet'",
)

TOTAL_REVENUE_FACT_MARKERS = (
  'name="us-gaap:revenues"',
  'name="us-gaap:salesrevenuenet"',
  'name="us-gaap:revenuefromcontractwithcustomerexcludingassessedtax"',
  'name="us-gaap:revenuefromcontractwithcustomerincludingassessedtax"',
  'name="us-gaap:totalrevenuesandotherincome"',
  "name='us-gaap:revenues'",
  "name='us-gaap:salesrevenuenet'",
  "name='us-gaap:revenuefromcontractwithcustomerexcludingassessedtax'",
  "name='us-gaap:revenuefromcontractwithcustomerincludingassessedtax'",
  "name='us-gaap:totalrevenuesandotherincome'",
)

CASHFLOW_SELECTION_MARKERS = (
  "consolidated statements of cash flows",
  "consolidated statement of cash flows",
  "cash flows from operating activities",
  "cash flows from investing activities",
  "cash flows from financing activities",
  "net cash provided by operating activities",
  "net cash used in investing activities",
  "net cash provided by (used in) financing activities",
  "net increase (decrease) in cash and cash equivalents",
  "net increase in cash and cash equivalents",
)

CASHFLOW_SELECTION_EXCLUSIONS = (
  "condensed statements of cash flows",
  "condensed statement of cash flows",
  "condensed statements of cash flow",
  "condensed statement of cash flow",
)

INCOME_STATEMENT_SELECTION_MARKERS = (
  "consolidated statements of operations",
  "consolidated statement of operations",
  "consolidated statements of income",
  "consolidated statement of income",
  "consolidated statements of earnings",
  "consolidated statement of earnings",
)

INCOME_STATEMENT_ROW_MARKERS = (
  "total revenue",
  "total revenues",
  "net sales",
  "net revenue",
  "gross profit",
  "net income",
)

INCOME_STATEMENT_SELECTION_EXCLUSIONS = (
  "condensed statements of operations",
  "condensed statement of operations",
  "condensed statements of income",
  "condensed statement of income",
  "unaudited",
)

def is_consolidated_cashflow_statement(table) -> bool:
  for rule in CONSOLIDATED_BALANCE_SHEET_RULES:
    if rule(table):
      return True
  return False

def _score_cashflow_candidate(serialized_html_table: str) -> int:
  text = (
    serialized_html_table
    .lower()
    .replace("’", "'")
    .replace("‘", "'")
    .replace("`", "'")
  )
  score = 0

  if "consolidated statements of cash flows" in text:
    score += 14
  elif "consolidated statement of cash flows" in text:
    score += 12

  for marker in CASHFLOW_SELECTION_MARKERS[2:]:
    if marker in text:
      score += 2

  year_hits = len(set(re.findall(r"\b20\d{2}\b", text)))
  if year_hits >= 2:
    score += 2
  if year_hits >= 3:
    score += 1

  tr_count = text.count("<tr")
  if tr_count >= 20:
    score += 4
  elif tr_count >= 12:
    score += 2

  for marker in CASHFLOW_SELECTION_EXCLUSIONS:
    if marker in text:
      score -= 8

  return score

def _select_cashflow_table(html_tables: List[str]) -> Optional[str]:
  if not html_tables:
    return None
  if len(html_tables) == 1:
    return html_tables[0]

  best_score = None
  best_table = None
  for table in html_tables:
    score = _score_cashflow_candidate(table)
    if best_score is None or score > best_score:
      best_score = score
      best_table = table

  if best_score is None or best_score <= 0:
    return None
  return best_table

def _collect_interest_supplemental_tables(html_text: str, selected_tables: List[str]) -> List[str]:
  from bs4 import BeautifulSoup

  selected_set = set(selected_tables)
  soup = BeautifulSoup(html_text, "html.parser")
  supplemental: List[str] = []
  for table in soup.find_all("table"):
    serialized = str(table)
    if serialized in selected_set:
      continue
    serialized_lower = serialized.lower()
    if any(marker in serialized_lower for marker in INTEREST_PAID_FACT_MARKERS):
      supplemental.append(serialized)
  return supplemental

def _table_context_text(table, limit: int = 20) -> str:
  texts: List[str] = []
  for node in table.find_all_previous(["p", "div"], limit=limit):
    if node.find("table") is not None:
      continue
    text = node.get_text(" ", strip=True)
    if text:
      texts.append(text)
  if not texts:
    return ""
  texts.reverse()
  return " ".join(texts)

def _score_total_revenue_candidate(table) -> int:
  serialized = str(table)
  text = (
    serialized
    .lower()
    .replace("’", "'")
    .replace("‘", "'")
    .replace("`", "'")
  )
  context = _table_context_text(table).lower()
  combined = f"{context} {text}"
  score = 0

  for marker in INCOME_STATEMENT_SELECTION_MARKERS:
    if marker in combined:
      score += 10
      break

  for marker in INCOME_STATEMENT_ROW_MARKERS:
    if marker in text:
      score += 2

  if any(marker in text for marker in TOTAL_REVENUE_FACT_MARKERS):
    score += 10

  year_hits = len(set(re.findall(r"\b20\d{2}\b", text)))
  if year_hits >= 2:
    score += 2
  if year_hits >= 3:
    score += 1

  tr_count = text.count("<tr")
  if tr_count >= 20:
    score += 3
  elif tr_count >= 10:
    score += 1

  for marker in INCOME_STATEMENT_SELECTION_EXCLUSIONS:
    if marker in combined:
      score -= 8

  return score

def _collect_total_revenue_supplemental_tables(html_text: str, selected_tables: List[str]) -> List[str]:
  from bs4 import BeautifulSoup

  soup = BeautifulSoup(html_text, "html.parser")
  selected_table_fragments: set[str] = set()
  for selected_html in selected_tables:
    selected_soup = BeautifulSoup(selected_html, "html.parser")
    for selected_table in selected_soup.find_all("table"):
      selected_table_fragments.add(str(selected_table))

  ranked_candidates: List[Tuple[int, str]] = []
  for table in soup.find_all("table"):
    serialized = str(table)
    if serialized in selected_table_fragments:
      continue
    score = _score_total_revenue_candidate(table)
    if score <= 0:
      continue
    ranked_candidates.append((score, serialized))

  ranked_candidates.sort(key=lambda item: item[0], reverse=True)
  selected: List[str] = []
  for _score, candidate in ranked_candidates:
    if candidate in selected:
      continue
    selected.append(candidate)
    if len(selected) >= 16:
      break
  return selected

def read_raw_cashflow_statements(inputs: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str]]:
  try:
    for company, path in inputs:
      original_filing = read_original_filing(company, path)

      if original_filing is None:
        yield company, f"{CASHFLOW_ERR_TEMPLATE} -- failed to read 10-K filing at {path}"
        continue

      _company, _filing_name, html_text = original_filing
      html_tables = extract_tables_from_html(html_text, is_consolidated_cashflow_statement)

      if len(html_tables) == 0:
        yield company, f"{CASHFLOW_ERR_TEMPLATE} -- failed to read Consolidated Cash Flow Statements from 10-K filing"
        continue

      selected_table = _select_cashflow_table(html_tables)
      if selected_table is None:
        yield company, f"{CASHFLOW_ERR_TEMPLATE} -- read too many Consolidated Cash Flow Statements from 10-K filing"
        continue

      selected_tables = [selected_table]
      selected_tables.extend(_collect_interest_supplemental_tables(html_text, selected_tables))
      selected_tables.extend(_collect_total_revenue_supplemental_tables(html_text, selected_tables))
      yield company, "".join(selected_tables)
  except Exception as e:
    yield company, f"{CASHFLOW_ERR_TEMPLATE} -- unexpected exception during Consolidated Cash Flow Statements read -- {e}"

def extract_metrics(inputs: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str]]:
  try:
    from bs4 import BeautifulSoup
    import pandas as pd

    for company, serialized_html_table in inputs:
      soup = BeautifulSoup(serialized_html_table, "html.parser")
      tables = soup.find_all("table")
      if not tables:
        yield company, f"{CASHFLOW_ERR_TEMPLATE} -- failed to parse Consolidated Cash Flow Statements HTML"
        continue

      primary_table = tables[0]
      net_income = METRIC_EXTRACT[RAW_FEATURES.NET_INCOME.value](primary_table)
      metrics = {
        RAW_FEATURES.NET_INCOME.value: dict(net_income) if net_income else {},
        RAW_FEATURES.INTEREST_EXPENSE.value: {},
        RAW_FEATURES.TAX_EXPENSE.value: {},
        RAW_FEATURES.TOTAL_REVENUE.value: {},
      }

      # Net income must come from the primary cashflow table only; supplemental
      # tables often include multi-year summary data that pollutes year windows.
      all_years = sorted(metrics[RAW_FEATURES.NET_INCOME.value].keys())
      if not all_years:
        all_years = extract_fiscal_years(primary_table)
      if not all_years:
        yield company, (
          f"{CASHFLOW_ERR_TEMPLATE} -- failed to detect fiscal years in Consolidated Cash Flow Statements"
        )
        continue
      target_years = set(all_years)

      total_revenue_candidates: List[dict[str, int]] = []
      for table in tables:
        for metric_name in (
          RAW_FEATURES.INTEREST_EXPENSE.value,
          RAW_FEATURES.TAX_EXPENSE.value,
        ):
          extractor = METRIC_EXTRACT[metric_name]
          metric_values = extractor(table)
          if not metric_values:
            continue
          if not metrics[metric_name]:
            metrics[metric_name] = dict(metric_values)
            continue
          for year, value in metric_values.items():
            if year not in metrics[metric_name]:
              metrics[metric_name][year] = value

        total_revenue_values = METRIC_EXTRACT[RAW_FEATURES.TOTAL_REVENUE.value](table)
        if total_revenue_values:
          total_revenue_candidates.append(dict(total_revenue_values))

      if total_revenue_candidates:
        def _revenue_candidate_rank(values: dict[str, int]) -> tuple[int, int, int]:
          overlap = len(target_years & set(values.keys()))
          max_year = max(
            (int(year) for year in values.keys() if str(year).isdigit()),
            default=0,
          )
          return overlap, len(values), max_year

        ranked_revenue_candidates = sorted(
          total_revenue_candidates,
          key=_revenue_candidate_rank,
          reverse=True,
        )
        merged_total_revenue: dict[str, int] = {}
        for candidate in ranked_revenue_candidates:
          for year in all_years:
            if year in candidate and year not in merged_total_revenue:
              merged_total_revenue[year] = candidate[year]
        if merged_total_revenue:
          metrics[RAW_FEATURES.TOTAL_REVENUE.value] = merged_total_revenue
        else:
          metrics[RAW_FEATURES.TOTAL_REVENUE.value] = ranked_revenue_candidates[0]

      # Keep the pipeline moving when cashflow metrics are not disclosed/extractable.
      for fallback_metric_name in (
        RAW_FEATURES.NET_INCOME.value,
        RAW_FEATURES.INTEREST_EXPENSE.value,
        RAW_FEATURES.TAX_EXPENSE.value,
        RAW_FEATURES.TOTAL_REVENUE.value,
      ):
        fallback_metric = metrics[fallback_metric_name]
        if not all_years:
          continue
        for year in all_years:
          if year not in fallback_metric or fallback_metric[year] is None:
            fallback_metric[year] = "N/A"

      missing_value = False
      for metric_name, metric_values in metrics.items():
        if metric_values is None or not metric_values:
          yield company, (
            f"{CASHFLOW_ERR_TEMPLATE} -- failed to extract metric {metric_name}"
          )
          missing_value = True
          break
        for year in all_years:
          if year not in metric_values or metric_values[year] is None:
            yield company, (
              f"{CASHFLOW_ERR_TEMPLATE} -- fiscal year {year} -- failed to extract metric {metric_name}"
            )
            missing_value = True
            break
        if missing_value:
          break
      if missing_value:
        continue
      df = pd.DataFrame.from_dict(metrics, orient="index").reindex(columns=all_years)
      yield company, df.to_csv(sep=";")
  except Exception as e:
    yield company, f"{CASHFLOW_ERR_TEMPLATE} -- unexpected exception during Consolidated Cash Flow Statements analysis -- {e}"
\n\n---\n\n
# src/raw_features/consolidated_balance_sheet_rules.py
import html
import re
from typing import Optional
from src.raw_features.constants import RAW_FEATURES

COMMON_STOCK_UNITS_KEY = RAW_FEATURES.COMMON_STOCK_UNITS.value
CURRENT_ASSETS_KEY = RAW_FEATURES.CURRENT_ASSETS.value
CURRENT_LIABILITIES_KEY = RAW_FEATURES.CURRENT_LIABILITIES.value
TOTAL_ASSETS_KEY = RAW_FEATURES.TOTAL_ASSETS.value
LONG_TERM_DEBT_KEY = RAW_FEATURES.LONG_TERM_DEBT.value
SHORT_TERM_DEBT_KEY = RAW_FEATURES.SHORT_TERM_DEBT.value
STOCKHOLDERS_EQUITY_KEY = RAW_FEATURES.STOCKHOLDERS_EQUITY.value
RETAINED_EARNINGS_KEY = RAW_FEATURES.RETAINED_EARNINGS.value

CORE_BALANCE_SHEET_TERMS = (
  "total current assets",
  "total current liabilities",
  "total assets",
)

EQUITY_TERMS = (
  "stockholders' equity",
  "stockholders equity",
  "shareholders' equity",
  "shareholders equity",
  "stockholders' deficit",
  "stockholders deficit",
  "shareholders' deficit",
  "shareholders deficit",
  "stockholders' deficit equity",
  "stockholders deficit equity",
  "shareholders' deficit equity",
  "shareholders deficit equity",
  "stockholders' equity deficit",
  "stockholders equity deficit",
  "shareholders' equity deficit",
  "shareholders equity deficit",
)

LIABILITIES_EQUITY_MARKERS = (
  "liabilities and stockholders' equity",
  "liabilities and stockholders equity",
  "liabilities and shareholders' equity",
  "liabilities and shareholders equity",
  "liabilities and equity",
  "liabilities and stockholders' deficit",
  "liabilities and stockholders deficit",
  "liabilities and shareholders' deficit",
  "liabilities and shareholders deficit",
  "liabilities and stockholders' deficit equity",
  "liabilities and stockholders deficit equity",
  "liabilities and shareholders' deficit equity",
  "liabilities and shareholders deficit equity",
  "liabilities and stockholders' equity deficit",
  "liabilities and stockholders equity deficit",
  "liabilities and shareholders' equity deficit",
  "liabilities and shareholders equity deficit",
)

BALANCE_SHEET_EXCLUSION_PATTERNS = (
  "weighted average useful lives",
  "total current assets acquired",
  "total current liabilities acquired",
  "total liabilities acquired",
  "net assets acquired",
  "purchase price allocation",
)

def _has_consolidated_balance_sheet_heading(text: str) -> bool:
  return (
    "consolidated" in text
    and "balance sheet" in text
  )

def _normalize_label_text(text: str) -> str:
  normalized = _normalize_metric_text(text)
  normalized = normalized.replace("(", " ").replace(")", " ")
  normalized = re.sub(r"\s+", " ", normalized).strip()
  return normalized

def _has_liabilities_equity_marker(table_text: str) -> bool:
  marker_text = _normalize_label_text(table_text)
  if any(marker in marker_text for marker in LIABILITIES_EQUITY_MARKERS):
    return True
  if re.search(
    r"liabilities\s+and\s+(?:stockholders'?|shareholders'?)\s+(?:deficit\s+)?equity\b",
    marker_text
  ):
    return True
  if re.search(
    r"liabilities\s+and\s+(?:stockholders'?|shareholders'?)\s+equity\s+deficit\b",
    marker_text
  ):
    return True
  return (
    "liabilities" in marker_text
    and any(term in marker_text for term in EQUITY_TERMS)
  )

def _context_text(table, limit: int = 20) -> str:
  texts: list[str] = []

  for node in table.find_all_previous(["p", "div"], limit=limit):
    if node.find("table") is not None:
      continue
    text = node.get_text(" ", strip=True)
    if text:
      texts.append(text)

  if not texts:
    return ""
  texts.reverse()
  return " ".join(texts)

def has_mandatory_metrics(table) -> bool:
  table_text = _normalize_metric_text(table.get_text(" ", strip=True))
  context = _normalize_metric_text(_context_text(table))
  combined = f"{context} {table_text}".strip()

  if any(pattern in combined for pattern in BALANCE_SHEET_EXCLUSION_PATTERNS):
    return False

  tr_count = len(table.find_all("tr"))
  if tr_count < 10:
    return False

  core_hits = sum(term in table_text for term in CORE_BALANCE_SHEET_TERMS)
  has_current_totals = (
    "total current assets" in table_text
    and "total current liabilities" in table_text
  )
  if core_hits < len(CORE_BALANCE_SHEET_TERMS) and not has_current_totals:
    return False

  has_liabilities_and_equity = _has_liabilities_equity_marker(table_text)
  if not has_liabilities_and_equity:
    return False

  if _has_consolidated_balance_sheet_heading(combined):
    return True

  return tr_count >= 16


def _expand_row_cells(row) -> list[str]:
  expanded = []
  for cell in row.find_all(["td", "th"]):
    colspan = int(cell.get("colspan", 1))
    text = cell.get_text(" ", strip=True)
    for _ in range(colspan):
      expanded.append(text)
  return expanded


def _year_by_column(table) -> dict[int, str]:
  for row in table.find_all("tr"):
    cols = _expand_row_cells(row)
    year_hits = [re.search(r"\b(20\d{2})\b", text) for text in cols]
    if sum(1 for hit in year_hits if hit) >= 2:
      return {
        idx: hit.group(1)
        for idx, hit in enumerate(year_hits)
        if hit is not None
      }
  return {}

def _year_headers(table) -> list[str]:
  for row in table.find_all("tr"):
    years: list[str] = []
    for cell in row.find_all(["td", "th"]):
      for match in re.findall(r"\b(20\d{2})\b", cell.get_text(" ", strip=True)):
        if match not in years:
          years.append(match)
    if len(years) >= 2:
      return years
  return []

def _target_years(table, year_by_col: dict[int, str]) -> list[str]:
  header_years = _year_headers(table)
  if header_years:
    return header_years
  if year_by_col:
    return sorted(set(year_by_col.values()))
  return []

def extract_fiscal_years(table) -> list[str]:
  year_by_col = _year_by_column(table)
  return _target_years(table, year_by_col)


def _parse_number(text: str) -> Optional[int]:
  match = re.search(r"\d[\d,]*", text)
  if not match:
    stripped = text.strip()
    if stripped and re.fullmatch(r"[—–-]+", stripped):
      return 0
    return None
  value = int(match.group(0).replace(",", ""))
  if "(" in text and ")" in text:
    return -value
  return value

def _cell_is_negative(cell, text: str) -> bool:
  normalized = text.strip()
  compact = re.sub(r"\s+", "", normalized)
  if re.fullmatch(r"\(?\$?\d[\d,]*(?:\.\d+)?\)?", compact):
    if compact.startswith("(") or compact.endswith(")"):
      return True
  if re.search(r"-\s*\d", normalized):
    return True
  for tag in cell.find_all(True):
    name = (tag.name or "").lower()
    if name.endswith("nonfraction") and tag.get("sign") == "-":
      return True
  return False

def _normalize_metric_text(text: str) -> str:
  normalized = text.lower()
  normalized = (
    normalized
    .replace("’", "'")
    .replace("‘", "'")
    .replace("`", "'")
    .replace("\u00a0", " ")
  )
  normalized = re.sub(r"\s+", " ", normalized).strip()
  return normalized

def _detect_units(text: str) -> Optional[str]:
  normalized = text.lower()
  patterns = (
    r"\$\s*(?:in\s*)?(thousands|millions|billions)",
    r"(thousands|millions|billions)\s+of\s+dollars",
    r"dollars?\s*(?:in\s*)?(thousands|millions|billions)",
    r"usd\s*(?:in\s*)?(thousands|millions|billions)",
    r"\bin\s+(thousands|millions|billions)\b",
  )
  for pattern in patterns:
    match = re.search(pattern, normalized)
    if match:
      unit = match.group(1)
      return f"$ in {unit}"
  return None

def _table_header_candidates(table) -> list[str]:
  rows = list(table.find_all("tr"))
  candidates: list[str] = []

  if table.caption is not None:
    caption_text = table.caption.get_text(" ", strip=True)
    if caption_text:
      candidates.append(caption_text)

  sibling_texts: list[str] = []
  for sibling in table.previous_siblings:
    if len(sibling_texts) >= 4:
      break
    name = getattr(sibling, "name", None)
    if name not in ("p", "div"):
      continue
    text = sibling.get_text(" ", strip=True)
    if text:
      sibling_texts.append(text)
  if sibling_texts:
    candidates.extend(reversed(sibling_texts))

  year_row_idx = None
  for idx, row in enumerate(rows):
    cols = _expand_row_cells(row)
    year_hits = [re.search(r"\b(20\d{2})\b", text) for text in cols]
    if sum(1 for hit in year_hits if hit) >= 2:
      year_row_idx = idx
      break

  for row in rows[:3]:
    row_text = row.get_text(" ", strip=True)
    if row_text:
      candidates.append(row_text)

  if year_row_idx is not None:
    year_row_text = rows[year_row_idx].get_text(" ", strip=True)
    if year_row_text:
      candidates.append(year_row_text)
    if year_row_idx > 0:
      prev_row_text = rows[year_row_idx - 1].get_text(" ", strip=True)
      if prev_row_text:
        candidates.append(prev_row_text)

  return candidates

def extract_units(table) -> dict[str, str]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
    return {}

  candidates = _table_header_candidates(table)
  unit = _detect_units(" ".join(candidates))
  if unit is None:
    return {}

  years = sorted(set(year_by_col.values()))
  return {year: unit for year in years}

def _detect_share_scale(table) -> int:
  normalized = _normalize_metric_text(" ".join(_table_header_candidates(table)))
  if normalized == "":
    return 1
  if "except share" in normalized or "except shares" in normalized:
    return 1
  if re.search(r"(?:^|\W)(?:dollars?|usd|\$)\s*(?:in\s*)?(thousands|millions|billions)\b", normalized):
    return 1

  match = re.search(r"\bin\s+(thousands|millions|billions)\b", normalized)
  if not match:
    return 1

  unit = match.group(1)
  if unit == "thousands":
    return 1000
  if unit == "millions":
    return 1000000
  if unit == "billions":
    return 1000000000
  return 1

def _strip_html_tags(text: str) -> str:
  stripped = re.sub(r"<[^>]+>", " ", text)
  return html.unescape(re.sub(r"\s+", " ", stripped)).strip()

def _parse_share_count_value(text: str) -> Optional[int]:
  normalized = _normalize_metric_text(_strip_html_tags(text))
  if normalized == "":
    return None
  if normalized in ("no", "none"):
    return 0
  if re.fullmatch(r"[—–-]+", normalized):
    return 0
  match = re.search(r"\d[\d,]*", normalized)
  if match is None:
    return None
  return int(match.group(0).replace(",", ""))

def _apply_fact_scale(value: int, scale: int) -> int:
  if scale > 0:
    return value * (10 ** scale)
  if scale < 0:
    return int(round(value / (10 ** abs(scale))))
  return value

def _is_allowed_common_stock_context(dimensions: tuple[str, ...]) -> bool:
  if not dimensions:
    return True
  return all(
    "classofstockaxis" in dimension or "classesofsharecapitalaxis" in dimension
    for dimension in dimensions
  )

def _parse_xbrl_contexts(html_text: str) -> dict[str, tuple[str, tuple[str, ...]]]:
  contexts: dict[str, tuple[str, tuple[str, ...]]] = {}
  context_pattern = re.compile(
    r"<(?:xbrli:)?context\b[^>]*\bid=\"([^\"]+)\"[^>]*>(.*?)</(?:xbrli:)?context>",
    re.IGNORECASE | re.DOTALL,
  )
  instant_pattern = re.compile(
    r"<(?:xbrli:)?instant>\s*(\d{4})-\d{2}-\d{2}\s*</(?:xbrli:)?instant>",
    re.IGNORECASE,
  )
  dimension_pattern = re.compile(
    r"<(?:xbrldi:)?explicitMember\b[^>]*\bdimension=\"([^\"]+)\"",
    re.IGNORECASE,
  )

  for context_id, body in context_pattern.findall(html_text):
    instant_match = instant_pattern.search(body)
    if instant_match is None:
      continue
    dimensions = tuple(
      dimension.lower()
      for dimension in dimension_pattern.findall(body)
    )
    contexts[context_id] = (instant_match.group(1), dimensions)

  return contexts

def _extract_xbrl_common_stock_units(
  html_text: str,
  fact_names: tuple[str, ...]
) -> dict[str, int]:
  if html_text.strip() == "":
    return {}

  contexts = _parse_xbrl_contexts(html_text)
  if not contexts:
    return {}

  accepted_fact_names = {name.lower() for name in fact_names}
  values_by_year: dict[str, dict[str, int]] = {}

  for fact_name in accepted_fact_names:
    fact_pattern = re.compile(
      rf"<(?:ix:)?nonFraction\b([^>]*)\bname=\"{re.escape(fact_name)}\"([^>]*)>(.*?)</(?:ix:)?nonFraction>",
      re.IGNORECASE | re.DOTALL,
    )
    for before_attrs, after_attrs, raw_value in fact_pattern.findall(html_text):
      attrs = f"{before_attrs} {after_attrs}"
      context_match = re.search(r"\bcontextRef=\"([^\"]+)\"", attrs, re.IGNORECASE)
      if context_match is None:
        continue
      context_ref = context_match.group(1)
      context = contexts.get(context_ref)
      if context is None:
        continue
      year, dimensions = context
      if not _is_allowed_common_stock_context(dimensions):
        continue
      value = _parse_share_count_value(raw_value)
      if value is None:
        continue
      scale_match = re.search(r"\bscale=\"(-?\d+)\"", attrs, re.IGNORECASE)
      scale = int(scale_match.group(1)) if scale_match is not None else 0
      values_by_year.setdefault(year, {})[context_ref] = _apply_fact_scale(value, scale)

  return {
    year: sum(context_values.values())
    for year, context_values in sorted(values_by_year.items())
    if context_values
  }

def _is_common_stock_units_row(row_text: str) -> bool:
  normalized = _normalize_label_text(row_text)
  if "outstanding" not in normalized:
    return False
  if "preferred" in normalized:
    return False
  if not any(marker in normalized for marker in ("common stock", "ordinary share", "class a", "class b")):
    return False
  return any(marker in normalized for marker in ("shares authorized", "par value", "issued and outstanding"))

def _extract_common_stock_units_from_row_text(row_text: str) -> dict[str, int]:
  normalized = _normalize_metric_text(html.unescape(row_text))
  results: dict[str, int] = {}

  paired_values_pattern = re.compile(
    r"\b(no|\d[\d,]*)\s+and\s+(no|\d[\d,]*)\s+shares?\s+(?:issued\s+and\s+)?outstanding(?:\s+as\s+of|\s+at)?"
    r".{0,80}?(20\d{2})\s+and\s+.{0,40}?(20\d{2}).{0,80}?\brespectively\b"
  )
  same_value_pattern = re.compile(
    r"\b(no|\d[\d,]*)\s+shares?\s+(?:issued\s+and\s+)?outstanding(?:\s+as\s+of|\s+at)?"
    r".{0,80}?(20\d{2})\s+and\s+.{0,40}?(20\d{2})(?:.{0,80}?\brespectively\b)?"
  )
  repeated_pattern = re.compile(
    r"\b(no|\d[\d,]*)\s+shares?\s+(?:issued\s+and\s+)?outstanding(?:\s+as\s+of|\s+at)?"
    r".{0,80}?(20\d{2})\b"
  )

  for first_value_text, second_value_text, first_year, second_year in paired_values_pattern.findall(normalized):
    first_value = _parse_share_count_value(first_value_text)
    second_value = _parse_share_count_value(second_value_text)
    if first_value is not None:
      results[first_year] = first_value
    if second_value is not None:
      results[second_year] = second_value

  if results:
    return results

  for value_text, first_year, second_year in same_value_pattern.findall(normalized):
    value = _parse_share_count_value(value_text)
    if value is None:
      continue
    results[first_year] = value
    results[second_year] = value

  if results:
    return results

  for value_text, year in repeated_pattern.findall(normalized):
    value = _parse_share_count_value(value_text)
    if value is None:
      continue
    results[year] = value

  return results

def _extract_common_stock_units_from_table(table) -> dict[str, int]:
  share_scale = _detect_share_scale(table)
  values_by_year: dict[str, int] = {}

  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True)
    if not _is_common_stock_units_row(row_text):
      continue
    row_values = _extract_common_stock_units_from_row_text(row_text)
    for year, value in row_values.items():
      scaled_value = value * share_scale
      values_by_year[year] = values_by_year.get(year, 0) + scaled_value

  return values_by_year

def _extract_cover_page_common_stock_units(html_text: str) -> dict[str, int]:
  normalized = _normalize_metric_text(_strip_html_tags(html_text))
  patterns = (
    re.compile(
      r"\b(?P<value>\d[\d,]*)\s+shares?\s+of\s+(?:common\s+stock|ordinary\s+shares?)\b.{0,160}?\bissued\s+and\s+outstanding\s+as\s+of\b.{0,80}?(?P<year>20\d{2})"
    ),
    re.compile(
      r"\bas\s+of\b.{0,80}?(?P<year>20\d{2}).{0,160}?\b(?:had|were)\s+(?P<value>\d[\d,]*)\s+outstanding\s+shares?\s+of\s+(?:common\s+stock|ordinary\s+shares?)"
    ),
    re.compile(
      r"\bnumber\s+of\s+(?:common|ordinary)\s+shares?.{0,120}?\boutstanding\s+as\s+of\b.{0,80}?(?P<year>20\d{2}).{0,80}?:\s*(?P<value>\d[\d,]*)\s+shares?"
    ),
    re.compile(
      r"\bindicate\s+the\s+number\s+of\s+shares?\s+outstanding\b.{0,120}?:\s*(?P<value>\d[\d,]*)\s+shares?\s+of\s+common\s+stock\b.{0,120}?\bas\s+of\b.{0,80}?(?P<year>20\d{2})"
    ),
  )

  for pattern in patterns:
    match = pattern.search(normalized)
    if match is None:
      continue
    year = match.group("year")
    value_text = match.group("value")
    value = _parse_share_count_value(value_text)
    if value is not None:
      return {year: value}

  return {}

def extract_common_stock_units(table, filing_html: str = "") -> dict[str, int]:
  xbrl_year_end_values = _extract_xbrl_common_stock_units(
    filing_html,
    ("us-gaap:CommonStockSharesOutstanding",),
  )
  if xbrl_year_end_values:
    return xbrl_year_end_values

  table_values = _extract_common_stock_units_from_table(table)
  if table_values:
    return table_values

  cover_page_xbrl_values = _extract_xbrl_common_stock_units(
    filing_html,
    ("dei:EntityCommonStockSharesOutstanding",),
  )
  if cover_page_xbrl_values:
    return cover_page_xbrl_values

  return _extract_cover_page_common_stock_units(filing_html)


def extract_current_assets(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _year_headers(table)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    if "total current assets" not in row_text:
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    return results
  return {}

def extract_current_liabilities(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _year_headers(table)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    if "total current liabilities" not in row_text:
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    return results
  return {}

def extract_total_assets(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _target_years(table, year_by_col)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}

  total_assets_fallback_markers = (
    "total liabilities and stockholders' equity",
    "total liabilities and stockholders equity",
    "total liabilities and shareholders' equity",
    "total liabilities and shareholders equity",
    "total liabilities and stockholders' deficit",
    "total liabilities and stockholders deficit",
    "total liabilities and shareholders' deficit",
    "total liabilities and shareholders deficit",
    "total liabilities and stockholders' deficit equity",
    "total liabilities and stockholders deficit equity",
    "total liabilities and shareholders' deficit equity",
    "total liabilities and shareholders deficit equity",
    "total liabilities and stockholders' equity deficit",
    "total liabilities and stockholders equity deficit",
    "total liabilities and shareholders' equity deficit",
    "total liabilities and shareholders equity deficit",
    "liabilities and stockholders' equity",
    "liabilities and stockholders equity",
    "liabilities and shareholders' equity",
    "liabilities and shareholders equity",
    "liabilities and stockholders' deficit",
    "liabilities and stockholders deficit",
    "liabilities and shareholders' deficit",
    "liabilities and shareholders deficit",
    "liabilities and stockholders' deficit equity",
    "liabilities and stockholders deficit equity",
    "liabilities and shareholders' deficit equity",
    "liabilities and shareholders deficit equity",
    "liabilities and stockholders' equity deficit",
    "liabilities and stockholders equity deficit",
    "liabilities and shareholders' equity deficit",
    "liabilities and shareholders equity deficit",
  )

  total_assets_xbrl_concepts = (
    "us-gaap:assets",
    "us-gaap:assetsnet",
    "us-gaap:liabilitiesandstockholdersequity",
    "us-gaap:liabilitiesandshareholdersequity",
  )

  def _is_unlabeled_numeric_total_row(row_text: str) -> bool:
    if any(ch.isalpha() for ch in row_text):
      return False
    if not re.search(r"\d", row_text):
      return False
    return "$" in row_text or "(" in row_text or ")" in row_text

  awaiting_total_assets_values = False
  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    marker_row_text = _normalize_label_text(row_text)
    row_html = str(row).lower()
    has_total_assets_xbrl = any(
      f'name="{concept}"' in row_html or f"name='{concept}'" in row_html
      for concept in total_assets_xbrl_concepts
    )
    has_total_assets_marker = (
      "total assets" in marker_row_text
      or any(marker in marker_row_text for marker in total_assets_fallback_markers)
    )
    candidate_by_split_layout = awaiting_total_assets_values and _is_unlabeled_numeric_total_row(row_text)
    if (
      not has_total_assets_marker
      and not has_total_assets_xbrl
      and not candidate_by_split_layout
    ):
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    if has_total_assets_marker or has_total_assets_xbrl:
      awaiting_total_assets_values = True
  return {}

def _is_stockholders_equity_row(row_text: str) -> bool:
  row_text = _normalize_metric_text(row_text)
  marker_row_text = _normalize_label_text(row_text)
  primary_markers = (
    "total stockholders' equity",
    "total stockholders equity",
    "total shareholders' equity",
    "total shareholders equity",
    "total stockholder equity",
    "total shareholder equity",
    "total common stockholders' equity",
    "total common stockholders equity",
    "total common shareholders' equity",
    "total common shareholders equity",
    "total stockholders' deficit",
    "total stockholders deficit",
    "total shareholders' deficit",
    "total shareholders deficit",
    "total common stockholders' deficit",
    "total common stockholders deficit",
    "total common shareholders' deficit",
    "total common shareholders deficit",
    "total stockholders' deficit equity",
    "total stockholders deficit equity",
    "total shareholders' deficit equity",
    "total shareholders deficit equity",
    "total common stockholders' deficit equity",
    "total common stockholders deficit equity",
    "total common shareholders' deficit equity",
    "total common shareholders deficit equity",
    "total stockholders' equity deficit",
    "total stockholders equity deficit",
    "total shareholders' equity deficit",
    "total shareholders equity deficit",
    "total common stockholders' equity deficit",
    "total common stockholders equity deficit",
    "total common shareholders' equity deficit",
    "total common shareholders equity deficit",
  )
  if any(marker in marker_row_text for marker in primary_markers):
    return True

  if re.search(
    r"\btotal\s+(?:common\s+)?(?:stockholders'?|shareholders'?)\s+(?:deficit\s+)?equity\b",
    marker_row_text
  ):
    return True
  if re.search(
    r"\btotal\s+(?:common\s+)?(?:stockholders'?|shareholders'?)\s+equity\s+deficit\b",
    marker_row_text
  ):
    return True

  if "total equity" in marker_row_text:
    return not any(
      marker in marker_row_text
      for marker in (
        "liabilities and equity",
        "liabilities & equity",
        "liabilities and shareholders",
        "liabilities and stockholders",
      )
    )
  return False

def extract_stockholders_equity(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _target_years(table, year_by_col)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}
  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    if not _is_stockholders_equity_row(row_text):
      continue
    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    return results
  return {}

def _is_retained_earnings_row(row_text: str) -> bool:
  normalized = _normalize_label_text(row_text)
  return any(
    marker in normalized
    for marker in (
      "retained earnings",
      "retained deficit",
      "accumulated deficit",
      "accumulated earnings",
    )
  )

def extract_retained_earnings(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _target_years(table, year_by_col)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}

  retained_earnings_fact_markers = (
    'name="us-gaap:retainedearningsaccumulateddeficit"',
    "name='us-gaap:retainedearningsaccumulateddeficit'",
    'name="us-gaap:retainedearnings"',
    "name='us-gaap:retainedearnings'",
  )

  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    row_html = str(row).lower()
    has_fact_marker = any(marker in row_html for marker in retained_earnings_fact_markers)
    if not has_fact_marker and not _is_retained_earnings_row(row_text):
      continue

    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        if _cell_is_negative(cell, cell_text):
          value = -abs(value)
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan
    if results:
      if not years:
        return results
      if all(year in results for year in years):
        return results
    if years and ordered_values:
      return {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    return results
  return {}

def _is_long_term_debt_keyword_row(row_text: str) -> bool:
  debt_keywords = (
    "long-term debt",
    "long term debt",
    "term debt",
    "long-term notes payable",
    "long term notes payable",
    "notes payable",
    "long-term borrowings",
    "long term borrowings",
    "borrowings",
    "convertible senior notes",
    "convertible notes",
    "senior notes",
    "debt, non-current",
    "debt, noncurrent",
    "non-current debt",
    "noncurrent debt",
  )
  return any(keyword in row_text for keyword in debt_keywords)


def _is_current_debt_row(row_text: str) -> bool:
  if "current portion" in row_text or "current maturit" in row_text:
    return not any(
      phrase in row_text
      for phrase in (
        "less current portion",
        "net of current portion",
        "excluding current portion",
        "less current maturit",
        "net of current maturit",
        "excluding current maturit",
      )
    )
  return any(
    marker in row_text
    for marker in ("short-term", "short term", "current debt")
  )

def _has_debt_marker(row_text: str) -> bool:
  return any(
    marker in row_text
    for marker in (
      "debt",
      "borrow",
      "notes payable",
      "note payable",
      "loan",
      "commercial paper",
      "credit facility",
      "line of credit",
    )
  )

def _is_short_term_debt_keyword_row(row_text: str) -> bool:
  debt_keywords = (
    "short-term debt",
    "short term debt",
    "short-term borrowings",
    "short term borrowings",
    "short-term notes payable",
    "short term notes payable",
    "short-term loans",
    "short term loans",
    "commercial paper",
    "bank borrowings",
    "bank loans",
    "revolving credit",
    "line of credit",
    "credit facility",
    "current debt",
    "current portion of debt",
    "current portion of long-term debt",
    "current portion of long term debt",
    "current portion of long-term borrowings",
    "current portion of long term borrowings",
    "current maturities of long-term debt",
    "current maturities of long term debt",
    "current maturities of long-term borrowings",
    "current maturities of long term borrowings",
  )
  if any(keyword in row_text for keyword in debt_keywords):
    return True
  if _is_current_debt_row(row_text):
    return _has_debt_marker(row_text)
  return False

def _short_term_debt_score(
  row_text: str,
  section: Optional[str],
  in_liabilities: bool
) -> Optional[int]:
  if not in_liabilities:
    return None
  if not _is_short_term_debt_keyword_row(row_text):
    return None
  if (
    "long-term" in row_text
    and "current portion" not in row_text
    and "current maturit" not in row_text
    and "short-term" not in row_text
    and "short term" not in row_text
  ):
    return None

  score = 0
  if section == "current":
    score += 2
  elif section in ("noncurrent", "after_current"):
    score -= 2

  if "short-term" in row_text or "short term" in row_text:
    score += 3
  if "current portion" in row_text or "current maturit" in row_text:
    score += 3
  if "commercial paper" in row_text:
    score += 2
  if "borrow" in row_text:
    score += 1
  if "notes payable" in row_text or "note payable" in row_text:
    score += 1
  if "debt" in row_text:
    score += 1

  if (
    "total" in row_text
    and "short-term" not in row_text
    and "short term" not in row_text
    and "current portion" not in row_text
    and "current maturit" not in row_text
  ):
    score -= 2

  return score


def _long_term_debt_score(
  row_text: str,
  section: Optional[str],
  in_liabilities: bool
) -> Optional[int]:
  if not in_liabilities:
    return None
  if not _is_long_term_debt_keyword_row(row_text):
    return None
  if _is_current_debt_row(row_text):
    return None

  score = 0
  if section in ("noncurrent", "after_current"):
    score += 2

  if any(
    marker in row_text
    for marker in ("long-term", "long term", "non-current", "noncurrent")
  ):
    score += 3

  if "term debt" in row_text:
    score += 2
  if "notes payable" in row_text:
    score += 2
  if "convertible" in row_text or "senior notes" in row_text:
    score += 1
  if "borrow" in row_text:
    score += 1
  if "debt" in row_text:
    score += 1

  if "total" in row_text and "long-term" not in row_text and "non-current" not in row_text and "noncurrent" not in row_text:
    score -= 2

  return score

def extract_long_term_debt(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _target_years(table, year_by_col)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}

  in_liabilities = False
  section: Optional[str] = None
  best_score: Optional[int] = None
  best_results: Optional[dict[str, int]] = None
  long_term_debt_like_in_liabilities = False

  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()

    if not in_liabilities and "liabilities" in row_text:
      in_liabilities = True

    if "total current liabilities" in row_text:
      section = "after_current"
    elif any(marker in row_text for marker in ("non-current liabilities", "noncurrent liabilities", "long-term liabilities", "long term liabilities")):
      section = "noncurrent"
    elif "current liabilities" in row_text:
      section = "current"

    score = _long_term_debt_score(row_text, section, in_liabilities)
    if score is None:
      continue
    long_term_debt_like_in_liabilities = True

    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan

    candidate_results: dict[str, int] = {}
    if results:
      if not years or all(year in results for year in years):
        candidate_results = results
    if not candidate_results and years and ordered_values:
      candidate_results = {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    if not candidate_results:
      continue

    if best_score is None or score > best_score or (score == best_score and section in ("noncurrent", "after_current")):
      best_score = score
      best_results = candidate_results

  if best_results is not None:
    return best_results

  if not long_term_debt_like_in_liabilities:
    return {year: 0 for year in years}

  return {}

def extract_short_term_debt(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  years = _target_years(table, year_by_col)
  if not year_by_col:
    year_by_col = {}
  if not year_by_col and not years:
    return {}

  in_liabilities = False
  section: Optional[str] = None
  best_score: Optional[int] = None
  best_results: Optional[dict[str, int]] = None
  debt_like_in_liabilities = False

  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()

    if not in_liabilities and "liabilities" in row_text:
      in_liabilities = True

    if "total current liabilities" in row_text:
      section = "after_current"
    elif any(marker in row_text for marker in ("non-current liabilities", "noncurrent liabilities", "long-term liabilities", "long term liabilities")):
      section = "noncurrent"
    elif "current liabilities" in row_text:
      section = "current"

    if in_liabilities and _is_short_term_debt_keyword_row(row_text):
      debt_like_in_liabilities = True

    score = _short_term_debt_score(row_text, section, in_liabilities)
    if score is None:
      continue

    results: dict[str, int] = {}
    ordered_values: list[int] = []
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      cell_text = cell.get_text(" ", strip=True)
      value = _parse_number(cell_text)
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
        if not any(ch.isalpha() for ch in cell_text):
          ordered_values.append(value)
      col_idx += colspan

    candidate_results: dict[str, int] = {}
    if results:
      if not years or all(year in results for year in years):
        candidate_results = results
    if not candidate_results and years and ordered_values:
      candidate_results = {
        year: ordered_values[idx]
        for idx, year in enumerate(years)
        if idx < len(ordered_values)
      }
    if not candidate_results:
      continue

    if best_score is None or score > best_score or (score == best_score and section == "current"):
      best_score = score
      best_results = candidate_results

  if best_results is not None:
    return best_results

  if not debt_like_in_liabilities:
    return {year: 0 for year in years}

  return {}

CONSOLIDATED_BALANCE_SHEET_RULES = [has_mandatory_metrics]

METRIC_EXTRACT = {
  COMMON_STOCK_UNITS_KEY: extract_common_stock_units,
  CURRENT_ASSETS_KEY: extract_current_assets,
  CURRENT_LIABILITIES_KEY: extract_current_liabilities,
  TOTAL_ASSETS_KEY: extract_total_assets,
  LONG_TERM_DEBT_KEY: extract_long_term_debt,
  SHORT_TERM_DEBT_KEY: extract_short_term_debt,
  STOCKHOLDERS_EQUITY_KEY: extract_stockholders_equity,
  RETAINED_EARNINGS_KEY: extract_retained_earnings,
}
\n\n---\n\n
# src/raw_features/consolidated_balance_sheet.py
import json
import re
from typing import Iterator, List, Optional, Tuple

from src.raw_features.consolidated_balance_sheet_rules import (
  CONSOLIDATED_BALANCE_SHEET_RULES,
  METRIC_EXTRACT,
  extract_common_stock_units,
  extract_fiscal_years,
  extract_units,
)
from src.raw_features.constants import BALANCE_SHEET_ERR_TEMPLATE, RAW_FEATURES
from src.raw_features.original_filing import extract_tables_from_html, read_original_filing

def is_consolidated_balance_sheet(table) -> bool:
  for rule in CONSOLIDATED_BALANCE_SHEET_RULES:
    if rule(table):
      return True
  return False

BALANCE_SHEET_SELECTION_MARKERS = (
  "consolidated balance sheets",
  "consolidated balance sheet",
  "liabilities and stockholders' equity",
  "liabilities and stockholders equity",
  "liabilities and shareholders' equity",
  "liabilities and shareholders equity",
  "liabilities and equity",
  "liabilities and stockholders' deficit",
  "liabilities and stockholders deficit",
  "liabilities and shareholders' deficit",
  "liabilities and shareholders deficit",
  "liabilities and stockholders' deficit equity",
  "liabilities and stockholders deficit equity",
  "liabilities and shareholders' deficit equity",
  "liabilities and shareholders deficit equity",
  "liabilities and stockholders' equity deficit",
  "liabilities and stockholders equity deficit",
  "liabilities and shareholders' equity deficit",
  "liabilities and shareholders equity deficit",
  "stockholders' equity",
  "stockholders equity",
  "shareholders' equity",
  "shareholders equity",
  "stockholders' deficit",
  "stockholders deficit",
  "shareholders' deficit",
  "shareholders deficit",
  "stockholders' deficit equity",
  "stockholders deficit equity",
  "shareholders' deficit equity",
  "shareholders deficit equity",
  "stockholders' equity deficit",
  "stockholders equity deficit",
  "shareholders' equity deficit",
  "shareholders equity deficit",
  "total liabilities",
  "total current assets",
  "total current liabilities",
  "total assets",
)

BALANCE_SHEET_SELECTION_EXCLUSIONS = (
  "weighted average useful lives",
  "total current assets acquired",
  "total current liabilities acquired",
  "total liabilities acquired",
  "net assets acquired",
  "purchase price allocation",
)

def _score_balance_sheet_candidate(serialized_html_table: str) -> int:
  text = (
    serialized_html_table
    .lower()
    .replace("’", "'")
    .replace("‘", "'")
    .replace("`", "'")
  )
  score = 0

  if "consolidated balance sheets" in text:
    score += 14
  elif "consolidated balance sheet" in text:
    score += 12

  for marker in BALANCE_SHEET_SELECTION_MARKERS[2:]:
    if marker in text:
      score += 2

  year_hits = len(set(re.findall(r"\b20\d{2}\b", text)))
  if year_hits >= 2:
    score += 2
  if year_hits >= 3:
    score += 1

  for marker in BALANCE_SHEET_SELECTION_EXCLUSIONS:
    if marker in text:
      score -= 8

  return score

def _select_balance_sheet_table(html_tables: List[str]) -> Optional[str]:
  if not html_tables:
    return None
  if len(html_tables) == 1:
    return html_tables[0]

  best_score = None
  best_table = None
  for table in html_tables:
    score = _score_balance_sheet_candidate(table)
    if best_score is None or score > best_score:
      best_score = score
      best_table = table

  if best_score is None or best_score <= 0:
    return None
  return best_table

def read_raw_balance_sheet(inputs: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str]]:
  try:
    for company, path in inputs:
      original_filing = read_original_filing(company, path)
      
      if original_filing is None:
        yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- failed to read 10-K filing at {path}"
        continue

      _company, _filing_name, html_text = original_filing
      html_tables = extract_tables_from_html(html_text, is_consolidated_balance_sheet)
      
      if len(html_tables) == 0:
        yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- failed to read Consolidated Balance Sheet from 10-K filing"
        continue

      selected_table = _select_balance_sheet_table(html_tables)
      if selected_table is None:
        yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- read too many Consolidated Balance Sheet from 10-K filing"
        continue

      yield company, json.dumps({
        "path": path,
        "table": selected_table,
      })
  except Exception as e:
    yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- unexpected exception during Consolidated Balance Sheet read -- {e}"

def extract_metrics(inputs: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str]]:
  try:
    from bs4 import BeautifulSoup
    import pandas as pd

    for company, serialized_html_table in inputs:
      filing_path: Optional[str] = None
      filing_html = ""
      try:
        payload = json.loads(serialized_html_table)
        filing_path = payload.get("path")
        serialized_html_table = payload.get("table", serialized_html_table)
      except (TypeError, json.JSONDecodeError):
        pass

      if filing_path:
        original_filing = read_original_filing(company, filing_path)
        if original_filing is not None:
          _company, _filing_name, filing_html = original_filing

      soup = BeautifulSoup(serialized_html_table, "html.parser")
      table = soup.find("table")
      if table is None:
        yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- failed to parse Consolidated Balance Sheet HTML"
        continue

      metrics = {
        RAW_FEATURES.COMMON_STOCK_UNITS.value: extract_common_stock_units(table, filing_html),
        RAW_FEATURES.CURRENT_ASSETS.value: METRIC_EXTRACT[RAW_FEATURES.CURRENT_ASSETS.value](table),
        RAW_FEATURES.CURRENT_LIABILITIES.value: METRIC_EXTRACT[RAW_FEATURES.CURRENT_LIABILITIES.value](table),
        RAW_FEATURES.SHORT_TERM_DEBT.value: METRIC_EXTRACT[RAW_FEATURES.SHORT_TERM_DEBT.value](table),
        RAW_FEATURES.LONG_TERM_DEBT.value: METRIC_EXTRACT[RAW_FEATURES.LONG_TERM_DEBT.value](table),
        RAW_FEATURES.RETAINED_EARNINGS.value: METRIC_EXTRACT[RAW_FEATURES.RETAINED_EARNINGS.value](table),
        RAW_FEATURES.STOCKHOLDERS_EQUITY.value: METRIC_EXTRACT[RAW_FEATURES.STOCKHOLDERS_EQUITY.value](table),
        RAW_FEATURES.TOTAL_ASSETS.value: METRIC_EXTRACT[RAW_FEATURES.TOTAL_ASSETS.value](table),
      }
      units_by_year = extract_units(table)
      all_years = sorted({year for metric in metrics.values() for year in metric.keys()})
      if not all_years:
        all_years = extract_fiscal_years(table)
      if not all_years:
        yield company, (
          f"{BALANCE_SHEET_ERR_TEMPLATE} -- failed to detect fiscal years in Consolidated Balance Sheet"
        )
        continue

      # Keep pipeline continuity when metrics are not disclosed/extractable.
      for metric_values in metrics.values():
        for year in all_years:
          if year not in metric_values or metric_values[year] is None:
            metric_values[year] = "N/A"

      df = pd.DataFrame.from_dict(metrics, orient="index").reindex(columns=all_years)
      if units_by_year:
        labeled_years = {
          year: f"{year} ({units_by_year[year]})"
          for year in all_years
          if year in units_by_year and units_by_year[year]
        }
        if labeled_years:
          df = df.rename(columns=labeled_years)
      yield company, df.to_csv(sep=";")
  except Exception as e:
    yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- unexpected exception during Consolidated Balance Sheet analysis -- {e}"
\n\n---\n\n
# src/raw_features/combined_metrics.py
from __future__ import annotations

import json
import re
from io import StringIO

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

FALLBACK_VALUES = {"", "n/a", "na", "none", "null", "nan", "-", "—"}

def parse_serialized_metrics(serialized_data_frame: str) -> pd.DataFrame:
  return pd.read_csv(StringIO(serialized_data_frame), sep=";", index_col=0)

def parse_job_key(job_key: str, job_key_separator: str) -> Tuple[str, str]:
  parts = job_key.split(job_key_separator, 1)
  if len(parts) != 2:
    return job_key, "unknown"
  return parts[0], parts[1]

def is_fallback_metric_value(value: object) -> bool:
  if value is None:
    return True
  try:
    if pd.isna(value):
      return True
  except TypeError:
    pass
  if isinstance(value, str):
    return value.strip().lower() in FALLBACK_VALUES
  return False

def parse_metric_column(column: object) -> Tuple[Optional[int], Optional[str]]:
  label = str(column).strip()
  match = re.match(r"^(\d{4})(?:\s*\((.+)\))?$", label)
  if not match:
    return None, None
  year = int(match.group(1))
  unit = match.group(2)
  if unit is not None:
    unit = " ".join(unit.split()).strip()
    if unit == "":
      unit = None
  return year, unit

def format_metric_column(year: int, unit: Optional[str]) -> str:
  if unit is None:
    return str(year)
  return f"{year} ({unit})"

def _normalize_metric_value(value: object) -> object:
  if hasattr(value, "item"):
    try:
      return value.item()
    except Exception:
      return value
  return value

def _parse_metrics_by_filing(
  metrics: Sequence[Tuple[str, str]],
  metric_type_name: str
) -> Dict[str, pd.DataFrame]:
  metrics_by_filing: Dict[str, pd.DataFrame] = {}
  for filing_key, serialized_data_frame in metrics:
    try:
      metrics_by_filing[filing_key] = parse_serialized_metrics(serialized_data_frame)
    except Exception as e:
      raise ValueError(
        f"Failed to parse {metric_type_name} DataFrame for filing key '{filing_key}' -- {e}"
      ) from e
  return metrics_by_filing

def merge_company_metric_frames(
  filing_metric_frames: List[Tuple[int, pd.DataFrame]],
  fiscal_year_threshold: int
) -> Dict[str, Dict[str, object]]:
  metric_values: Dict[str, Dict[int, object]] = {}
  metric_units: Dict[str, Dict[int, Optional[str]]] = {}
  metric_order: List[str] = []

  for _filing_year, filing_df in sorted(filing_metric_frames, key=lambda item: item[0]):
    for metric_name in filing_df.index:
      if metric_name not in metric_values:
        metric_values[metric_name] = {}
        metric_units[metric_name] = {}
        metric_order.append(metric_name)

      row = filing_df.loc[metric_name]
      metric_values_by_year = metric_values[metric_name]
      metric_units_by_year = metric_units[metric_name]
      for raw_column, metric_value in row.items():
        metric_year, metric_unit = parse_metric_column(raw_column)
        if metric_year is None:
          continue
        if metric_year < fiscal_year_threshold:
          continue

        if metric_unit is not None and metric_year not in metric_units_by_year:
          metric_units_by_year[metric_year] = metric_unit

        normalized_metric_value = _normalize_metric_value(metric_value)
        if is_fallback_metric_value(metric_value):
          if metric_year not in metric_values_by_year:
            metric_values_by_year[metric_year] = normalized_metric_value
          continue

        metric_values_by_year[metric_year] = normalized_metric_value
        if metric_unit is not None:
          metric_units_by_year[metric_year] = metric_unit

  consolidated_rows: Dict[str, Dict[str, object]] = {}
  for metric_name in metric_order:
    metric_row: Dict[str, object] = {}
    metric_values_by_year = metric_values[metric_name]
    metric_units_by_year = metric_units[metric_name]
    for metric_year in sorted(metric_values_by_year.keys()):
      metric_value = metric_values_by_year[metric_year]
      metric_unit = metric_units_by_year.get(metric_year)
      label = format_metric_column(metric_year, metric_unit)
      metric_row[label] = metric_value
    if metric_row:
      consolidated_rows[metric_name] = metric_row

  return consolidated_rows

def combine_metrics(
  balance_sheet_metrics: Sequence[Tuple[str, str]],
  cashflow_metrics: Sequence[Tuple[str, str]],
  fiscal_year_threshold: int,
  job_key_separator: str = "@"
) -> List[Tuple[str, str]]:
  balance_sheet_df_by_filing = _parse_metrics_by_filing(
    balance_sheet_metrics,
    "Consolidated Balance Sheet metrics"
  )
  cashflow_df_by_filing = _parse_metrics_by_filing(
    cashflow_metrics,
    "Consolidated Cash Flow metrics"
  )

  combined_metrics_by_filing: Dict[str, pd.DataFrame] = {}
  filing_keys = sorted(
    set(balance_sheet_df_by_filing.keys()) | set(cashflow_df_by_filing.keys())
  )
  for filing_key in filing_keys:
    data_frames: List[pd.DataFrame] = []
    balance_sheet_df = balance_sheet_df_by_filing.get(filing_key)
    if balance_sheet_df is not None:
      data_frames.append(balance_sheet_df)
    cashflow_df = cashflow_df_by_filing.get(filing_key)
    if cashflow_df is not None:
      data_frames.append(cashflow_df)

    if not data_frames:
      continue

    combined_metrics_by_filing[filing_key] = pd.concat(data_frames, axis=0, sort=False)

  filings_by_company: Dict[str, List[Tuple[int, pd.DataFrame]]] = {}
  for filing_key, filing_df in combined_metrics_by_filing.items():
    company, fiscal_year = parse_job_key(filing_key, job_key_separator)
    try:
      filing_year = int(fiscal_year)
    except ValueError:
      continue
    filings_by_company.setdefault(company, []).append((filing_year, filing_df))

  combined_metrics: List[Tuple[str, str]] = []
  for company in sorted(filings_by_company.keys()):
    consolidated_metrics = merge_company_metric_frames(
      filings_by_company[company],
      fiscal_year_threshold
    )
    if not consolidated_metrics:
      continue
    payload = {"ticker": company}
    payload.update(consolidated_metrics)
    combined_metrics.append((company, json.dumps(payload)))

  return combined_metrics
\n\n---\n\n
# spark_ml.py
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- SPARK INIT ---
spark = SparkSession.builder \
    .appName("Financial_Prediction_ML") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- 1. LOAD DATA ---
print("Loading processed data...")
df = spark.read.parquet("financial_training_data.parquet")

# --- 2. PREPARE FEATURES ---
# These input columns match the formulas from your image
input_cols = [
    "current_ratio", 
    "net_profit_margin", 
    "roe", 
    "roa", 
    "debt_to_equity",
    "debt_to_assets"
]

# Combine all columns into a single vector column named "features"
assembler = VectorAssembler(inputCols=input_cols, outputCol="features_raw")
df_vector = assembler.transform(df)

# Standardize features (Scale them so large numbers don't dominate)
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
scaler_model = scaler.fit(df_vector)
df_ready = scaler_model.transform(df_vector)

# Select only relevant columns
data = df_ready.select("ticker", "year", "features", "label")

# --- 3. TRAIN/TEST SPLIT ---
# 80% for training, 20% for testing
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

print(f"Training on {train_data.count()} records.")
print(f"Testing on {test_data.count()} records.")

# --- 4. MODEL TRAINING (Random Forest) ---
# We use Random Forest because it works well with tabular financial data
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)
rf_model = rf.fit(train_data)

# --- 5. EVALUATION ---
predictions = rf_model.transform(test_data)

print("\n--- Predictions Preview ---")
predictions.select("ticker", "year", "label", "prediction", "probability").show(5)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(f"\nModel Accuracy: {accuracy*100:.2f}%")

# Show which financial ratio was most important
print("\n--- Feature Importance ---")
importances = rf_model.featureImportances
for i, col_name in enumerate(input_cols):
    print(f"{col_name}: {importances[i]:.4f}")

spark.stop()\n\n---\n\n
# spark_etl.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lead, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.window import Window

# --- SPARK INIT ---
# We use the standard SparkSession builder
spark = SparkSession.builder \
    .appName("Financial_ETL_Project") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- SCHEMA DEFINITION ---
# Must match the JSON sent by the producer
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("report_date", StringType(), True),
    StructField("total_revenue", FloatType(), True),
    StructField("net_income", FloatType(), True),
    StructField("current_assets", FloatType(), True),
    StructField("current_liabilities", FloatType(), True),
    StructField("total_assets", FloatType(), True),
    StructField("total_liabilities", FloatType(), True),
    StructField("stockholders_equity", FloatType(), True),
    StructField("interest_expense", FloatType(), True),
    StructField("ebit", FloatType(), True),
    StructField("close_price", FloatType(), True)
])

# --- 1. READ FROM KAFKA ---
# We use 'read' (Batch) instead of 'readStream' here to allow 
# complex Window functions (looking at future prices) for training data generation.
print("Reading data from Kafka...")
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_reports_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON payload (value is binary in Kafka)
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# --- 2. FEATURE ENGINEERING (Formulas from your Image) ---
print("Calculating Financial Ratios...")

# Avoid division by zero issues
df_features = df_parsed.na.fill(0.0)

df_ratios = df_features \
    .withColumn("current_ratio", col("current_assets") / col("current_liabilities")) \
    .withColumn("net_profit_margin", col("net_income") / col("total_revenue")) \
    .withColumn("roe", col("net_income") / col("stockholders_equity")) \
    .withColumn("roa", col("net_income") / col("total_assets")) \
    .withColumn("debt_to_equity", col("total_liabilities") / col("stockholders_equity")) \
    .withColumn("debt_to_assets", col("total_liabilities") / col("total_assets"))

# --- 3. CREATE TARGET LABEL (Supervised Learning) ---
# Logic: Compare current year's price with next year's price.
# We define a Window partitioned by Ticker and ordered by Year.
windowSpec = Window.partitionBy("ticker").orderBy("year")

# Look ahead 1 year to get the future price
df_labeled = df_ratios.withColumn("next_year_price", lead("close_price", 1).over(windowSpec))

# Calculate Percentage Return
df_final = df_labeled.withColumn("yearly_return", 
                                 (col("next_year_price") - col("close_price")) / col("close_price"))

# DEFINE 'HEALTHY' (Label = 1):
# If the stock grew by more than 5% (0.05) next year, we consider the current report "Healthy".
df_dataset = df_final.withColumn("label", when(col("yearly_return") > 0.05, 1.0).otherwise(0.0))

# Remove rows where we don't have next year's data (the most recent year)
df_dataset = df_dataset.filter(col("next_year_price").isNotNull())

# Clean up infinite values created by division by zero
df_dataset = df_dataset.na.drop()

print("Preview of Training Data:")
df_dataset.select("ticker", "year", "roe", "debt_to_equity", "label").show(5)

# --- 4. SAVE TO STORAGE ---
# Save as Parquet for the ML model to read efficiently
output_path = "financial_training_data.parquet"
df_dataset.write.mode("overwrite").parquet(output_path)
print(f"Data saved to {output_path}")

spark.stop()\n\n---\n\n
# spark_altman_zprime_etl.py
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
    
    query.awaitTermination()\n\n---\n\n
# spark_altman_etl.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, avg, stddev, abs, rank, desc
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.window import Window

# --- CONFIGURATION CONSTANTS ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "financial_reports_stream"
SHUFFLE_PARTITIONS = "5"  # Low partition count for local testing efficiency

def get_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns the Spark Session with necessary configurations.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_schema() -> StructType:
    """
    Defines the schema for the incoming JSON data from Kafka.
    Matches the CSV structure provided by the ingestion layer.
    """
    return StructType([
        StructField("ticker", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("total_assets", FloatType(), True),
        StructField("current_assets", FloatType(), True),
        StructField("current_liabilities", FloatType(), True),
        StructField("total_liabilities", FloatType(), True),
        StructField("retained_earnings", FloatType(), True),
        StructField("ebit", FloatType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("total_revenue", FloatType(), True)
    ])

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Reads the raw stream from Kafka and parses the JSON payload.
    """
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON using the defined schema
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), get_schema()).alias("data")
    ).select("data.*")
    
    return parsed_stream

def calculate_altman_features(df: DataFrame) -> DataFrame:
    """
    Feature Engineering Step 1:
    Calculates the 5 coefficients (X1 - X5) required for the Altman Z-Score model.
    """
    return df.withColumn(
        "working_capital", col("current_assets") - col("current_liabilities")
    ).withColumn(
        "X1", col("working_capital") / col("total_assets")
    ).withColumn(
        "X2", col("retained_earnings") / col("total_assets")
    ).withColumn(
        "X3", col("ebit") / col("total_assets")
    ).withColumn(
        "X4", col("market_cap") / col("total_liabilities")
    ).withColumn(
        "X5", col("total_revenue") / col("total_assets")
    )

def compute_z_score(df: DataFrame) -> DataFrame:
    """
    Feature Engineering Step 2:
    Applies the linear Altman Formula (1968) to compute the Z-Score.
    Formula: Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)
    """
    return df.withColumn(
        "Z_Score", 
        round(
            (lit(1.2) * col("X1")) + 
            (lit(1.4) * col("X2")) + 
            (lit(3.3) * col("X3")) + 
            (lit(0.6) * col("X4")) + 
            (lit(1.0) * col("X5")), 
            2
        )
    )

def enrich_with_analytics(df: DataFrame) -> DataFrame:
    """
    Advanced Analytics Step:
    Calculates dynamic sector averages, standard deviations, and assigns Health Zones.
    """
    stats_window = Window.partitionBy("year")

    # 1. Calculate Statistics (Avg & StdDev) per Year
    df_stats = df \
        .withColumn("Yearly_Avg_Z", round(avg("Z_Score").over(stats_window), 2)) \
        .withColumn("Yearly_StdDev", round(stddev("Z_Score").over(stats_window), 2)) \
        .na.fill(0.0, ["Yearly_StdDev"])

    # 2. Apply Logic: Benchmarking, Anomaly Detection, and Classification
    return df_stats.withColumn(
        "Performance", 
        when(col("Z_Score") > col("Yearly_Avg_Z"), "Outperforming")
        .otherwise("Underperforming")
    ).withColumn(
        "Is_Anomaly",
        when(abs(col("Z_Score") - col("Yearly_Avg_Z")) > (lit(2) * col("Yearly_StdDev")), "YES")
        .otherwise("No")
    ).withColumn(
        "Health_Zone",
        when(col("Z_Score") > 2.99, "Safe (Green)")
        .when((col("Z_Score") >= 1.81) & (col("Z_Score") <= 2.99), "Grey (Caution)")
        .otherwise("Distress (Red)")
    )

def apply_top5_ranking(df: DataFrame) -> DataFrame:
    """
    Ranking Step:
    Assigns a rank to each company based on Z-Score within its Year and keeps only Top 5.
    """
    rank_window = Window.partitionBy("year").orderBy(desc("Z_Score"))
    
    return df.withColumn("Rank", rank().over(rank_window)) \
             .filter(col("Rank") <= 5)

def format_final_output(df: DataFrame) -> DataFrame:
    """
    Selects and orders the final columns for the presentation layer.
    """
    return df.select(
        "year", "Rank", "ticker", "Z_Score", "Health_Zone", 
        "Performance", "Is_Anomaly", "Yearly_Avg_Z"
    ).orderBy("year", "Rank")

def write_to_console(df: DataFrame):
    """
    Writes the streaming DataFrame to the console.
    Uses 'complete' mode to support dynamic re-ranking (Leaderboard logic).
    """
    query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    query.awaitTermination()

# --- MAIN EXECUTION FLOW ---
if __name__ == "__main__":
    print("Starting Financial Health Analysis System...")
    
    # 1. Init
    spark = get_spark_session("Altman_Z_Score_Modular_System")
    
    # 2. Ingest
    stream_df = read_kafka_stream(spark)
    
    # 3. Process (ETL & Logic)
    features_df = calculate_altman_features(stream_df)
    scored_df = compute_z_score(features_df)
    
    # 4. Analyze (Big Data Capabilities)
    enriched_df = enrich_with_analytics(scored_df)
    
    # 5. Rank (Leaderboard)
    top5_df = apply_top5_ranking(enriched_df)
    
    # 6. Format
    final_df = format_final_output(top5_df)
    
    # 7. Output
    write_to_console(final_df)\n\n---\n\n
# produser.py
import time
import json
import yfinance as yf
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'financial_reports_stream'
# List of tech companies to analyze
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'INTC', 'NVDA', 'CSCO', 'ORCL', 'IBM', 'AMD']

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_financials(ticker):
    """
    Fetches financial data and historical price for a given ticker.
    Returns a list of dictionaries (one per year).
    """
    print(f"Fetching data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        
        # Fetch Balance Sheet and Income Statement
        balance_sheet = stock.balance_sheet
        financials = stock.financials
        # Fetch history for price labels
        history = stock.history(period="max")
        
        if balance_sheet.empty or financials.empty:
            return []

        data_points = []
        dates = financials.columns
        
        for date in dates:
            try:
                date_str = str(date.date())
                year = date.year
                
                # Helper to safely extract value from DF
                def get_val(df, key):
                    try:
                        return float(df.loc[key, date])
                    except KeyError:
                        return 0.0

                # Get stock price at the time of the report
                close_price = 0.0
                if date_str in history.index:
                    close_price = float(history.loc[date_str]['Close'])
                else:
                    # Fallback: average of that year
                    yearly_data = history[history.index.year == year]
                    if not yearly_data.empty:
                        close_price = float(yearly_data['Close'].mean())

                # Construct the data object based on your IMAGE requirements
                record = {
                    'ticker': ticker,
                    'year': year,
                    'report_date': date_str,
                    # --- Raw Values for Ratios ---
                    'total_revenue': get_val(financials, 'Total Revenue'),
                    'net_income': get_val(financials, 'Net Income'),
                    'current_assets': get_val(balance_sheet, 'Current Assets'),
                    'current_liabilities': get_val(balance_sheet, 'Current Liabilities'),
                    'total_assets': get_val(balance_sheet, 'Total Assets'),
                    'total_liabilities': get_val(balance_sheet, 'Total Liabilities Net Minority Interest'),
                    'stockholders_equity': get_val(balance_sheet, 'Stockholders Equity'),
                    'interest_expense': get_val(financials, 'Interest Expense'), # Needed for Coverage Ratio
                    'ebit': get_val(financials, 'EBIT'), # Earnings Before Interest & Taxes
                    # --- Price for Labeling ---
                    'close_price': close_price
                }
                data_points.append(record)
            except Exception as e:
                print(f"Skipping year {year} for {ticker}: {e}")
                
        return data_points
        
    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")
        return []

if __name__ == "__main__":
    print("--- Starting Data Ingestion ---")
    for ticker in TICKERS:
        reports = get_financials(ticker)
        for report in reports:
            # Sending to Kafka
            producer.send(TOPIC_NAME, value=report)
            print(f"Sent: {ticker} - {report['year']}")
            # Simulate real-time streaming delay
            time.sleep(0.2)
            
    producer.flush()
    print("--- Data Ingestion Complete ---")\n\n---\n\n
# raw_features_spark_publisher.py
"""Goal: examine original 10-K reports in assets/filings_10k and publish .parquet
objects to Kafka as RAW_FEATURE DataFrames (see constants.py).
"""

from __future__ import annotations

import os
import re
import json

from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from src.raw_features.combined_metrics import combine_metrics, parse_metric_column
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
from src.raw_features.price import get_price

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

def build_year_first_publish_records(
  combined_metrics: List[Tuple[str, str]]
) -> List[Tuple[int, str, str]]:
  rows_by_year: Dict[int, List[Tuple[str, str]]] = {}

  for company, serialized_metrics in combined_metrics:
    try:
      payload = json.loads(serialized_metrics)
    except json.JSONDecodeError:
      continue

    ticker = str(payload.get("ticker", payload.get("ticket", company))).lower().strip()
    metrics_by_year: Dict[int, Dict[str, object]] = {}

    for metric_name, metric_values in payload.items():
      if metric_name in ("ticker", "ticket"):
        continue
      if not isinstance(metric_values, dict):
        continue
      for column_label, metric_value in metric_values.items():
        year, unit = parse_metric_column(column_label)
        if year is None:
          continue
        row = metrics_by_year.setdefault(year, {"ticker": ticker, "year": year})
        row[metric_name] = metric_value
        if unit:
          units = row.setdefault("units", {})
          if isinstance(units, dict) and metric_name not in units:
            units[metric_name] = unit

    for year, year_payload in metrics_by_year.items():
      year_payload["price"] = get_price(ticker, year)
      units = year_payload.setdefault("units", {})
      if not isinstance(units, dict):
        units = {}
        year_payload["units"] = units
      units["price"] = "$"
      rows_by_year.setdefault(year, []).append(
        (ticker, json.dumps(year_payload))
      )

  ordered_records: List[Tuple[int, str, str]] = []
  for year in sorted(rows_by_year.keys()):
    for ticker, serialized_payload in sorted(rows_by_year[year], key=lambda item: item[0]):
      ordered_records.append((year, ticker, serialized_payload))
  return ordered_records

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

  publish_records = build_year_first_publish_records(combined_metrics)
  if not publish_records:
    log_message("No yearly records were generated from combined metrics", "ERROR")
    return

  log_message(
    "Publishing %s yearly raw-feature records to kafka channel '%s' in year-first order" % (
      str(len(publish_records)),
      kafka_channel
    )
  )

  for year, company, serialized_metrics in publish_records:
    kafka_producer.produce(kafka_channel, key=company, value=serialized_metrics)
    kafka_producer.poll(0)
    log_message(
      "Published metrics for fiscal year %s -- company '%s' to kafka channel '%s'" % (
        str(year),
        company.upper(),
        kafka_channel
      )
    )

  kafka_producer.flush()
  
  spark.stop()

if __name__ == "__main__":
  main()
\n\n---\n\n
# logs/README.md
# Logs Directory

`logs/` contains runtime logs generated by local pipeline jobs.

Expected files:

```text
logs/
  README.md
  raw_features_spark_publisher.log
```

`raw_features_spark_publisher.log`:
- Created by `src/raw_features/logger.py`.
- Overwritten at process start, then appended during execution.
- Line format: `YYYY-MM-DD HH:MM:SS -- RawFeaturesSparkPublisher -- <LEVEL> -- <message>`.
- Typical levels: `INFO`, `ERROR`, `DEBUG`.

All log output files in this directory are ignored by Git, except this `README.md`.
\n\n---\n\n
# README.md
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
\n\n---\n\n
# .gitignore
__pycache__/
*.py[cod]

assets/filings_10k/*
!assets/filings_10k/README.md
!assets/filings_10k/price_cache.json

bash_scripts

logs/*
!logs/README.md
\n\n---\n\n
# assets/README.md
# Assets Directory Structure

`assets/` stores local data inputs used by the extraction pipeline.

Expected structure:

```text
assets/
  README.md
  filings_10k/
    <ticker>/
      filing-YYYY-MM-DD.html
      filing-YYYY-MM-DD.htm
  raw_html_tables/   (optional, temporary/debug artifacts)
```

Notes:
- `filings_10k/` contains one folder per ticker/company (`aapl`, `amd`, etc.).
- Filing files are expected to match the name pattern `filing-YYYY-*` so fiscal year can be parsed.
- `filings_10k/` is intentionally ignored by Git because it is large and environment-specific.
- For `raw_features_spark_publisher.py`, set `RAW_FEATURES_SPARK_PUBLISHER_ASSETS` to an absolute path that points to `assets/filings_10k`.
\n\n---\n\n
# assets/filings_10k/price_cache.json
{
  "AAOI": {
    "2015": 17.16,
    "2016": 23.44,
    "2017": 37.82,
    "2018": 15.43,
    "2019": 11.88,
    "2020": 8.51,
    "2021": 5.14,
    "2022": 1.89,
    "2023": 19.32,
    "2024": 36.86
  },
  "AAPL": {
    "2015": 26.315,
    "2016": 28.955,
    "2017": 42.3075,
    "2018": 39.435,
    "2019": 73.4125,
    "2020": 132.69,
    "2021": 177.57,
    "2022": 129.93,
    "2023": 192.53,
    "2024": 250.42,
    "2025": 271.86
  },
  "ACIW": {
    "2015": 21.4,
    "2016": 18.15,
    "2017": 22.67,
    "2018": 27.67,
    "2019": 37.89,
    "2020": 38.43,
    "2021": 34.7,
    "2022": 23.0,
    "2023": 30.6,
    "2024": 51.91
  },
  "ACLS": {
    "2015": 10.36,
    "2016": 14.55,
    "2017": 28.7,
    "2018": 17.8,
    "2019": 24.1,
    "2020": 29.12,
    "2021": 74.56,
    "2022": 79.36,
    "2023": 129.69,
    "2024": 69.87
  },
  "ACMR": {
    "2016": "N/A",
    "2017": 1.75,
    "2018": 3.6267,
    "2019": 6.15,
    "2020": 27.0833,
    "2021": 28.4233,
    "2022": 7.71,
    "2023": 19.54,
    "2024": 15.1
  },
  "ACN": {
    "2015": 104.5,
    "2016": 117.13,
    "2017": 153.09,
    "2018": 141.01,
    "2019": 210.57,
    "2020": 261.21,
    "2021": 414.55,
    "2022": 266.84,
    "2023": 350.91,
    "2024": 351.79,
    "2025": 268.3
  },
  "ADBE": {
    "2015": 93.94,
    "2016": 102.95,
    "2017": 175.24,
    "2018": 226.24,
    "2019": 329.81,
    "2020": 500.12,
    "2021": 567.06,
    "2022": 336.53,
    "2023": 596.6,
    "2024": 444.68,
    "2025": 349.99
  },
  "ADI": {
    "2015": 55.32,
    "2016": 72.62,
    "2017": 89.03,
    "2018": 85.83,
    "2019": 118.84,
    "2020": 147.73,
    "2021": 175.77,
    "2022": 164.03,
    "2023": 198.56,
    "2024": 212.46,
    "2025": 271.2
  },
  "ADP": {
    "2015": 84.72,
    "2016": 102.78,
    "2017": 117.19,
    "2018": 131.12,
    "2019": 170.5,
    "2020": 176.2,
    "2021": 246.58,
    "2022": 238.86,
    "2023": 232.97,
    "2024": 292.73,
    "2025": 257.23
  },
  "ADSK": {
    "2015": 60.93,
    "2016": 74.01,
    "2017": 104.83,
    "2018": 128.61,
    "2019": 183.46,
    "2020": 305.34,
    "2021": 281.19,
    "2022": 186.87,
    "2023": 243.48,
    "2024": 295.57,
    "2025": 296.01
  },
  "AGYS": {
    "2015": 9.99,
    "2016": 10.36,
    "2017": 12.28,
    "2018": 14.34,
    "2019": 25.41,
    "2020": 38.38,
    "2021": 44.46,
    "2022": 79.14,
    "2023": 84.82,
    "2024": 131.71,
    "2025": 118.84
  },
  "AKAM": {
    "2015": 52.63,
    "2016": 66.68,
    "2017": 65.04,
    "2018": 61.08,
    "2019": 86.38,
    "2020": 104.99,
    "2021": 117.04,
    "2022": 84.3,
    "2023": 118.35,
    "2024": 95.65
  },
  "AMAT": {
    "2015": 18.67,
    "2016": 32.27,
    "2017": 51.12,
    "2018": 32.74,
    "2019": 61.04,
    "2020": 86.3,
    "2021": 157.36,
    "2022": 97.38,
    "2023": 162.07,
    "2024": 162.63,
    "2025": 256.99
  },
  "AMBA": {
    "2015": 55.74,
    "2016": 54.13,
    "2017": 58.75,
    "2018": 34.98,
    "2019": 60.56,
    "2020": 91.82,
    "2021": 202.89,
    "2022": 82.23,
    "2023": 61.29,
    "2024": 72.74,
    "2025": 70.84
  },
  "AMD": {
    "2015": 2.87,
    "2016": 11.34,
    "2017": 10.28,
    "2018": 18.46,
    "2019": 45.86,
    "2020": 91.71,
    "2021": 143.9,
    "2022": 64.77,
    "2023": 147.41,
    "2024": 120.79
  }
}
\n\n---\n\n
# assets/filings_10k/README.md
# Filings-10K Directory Structure