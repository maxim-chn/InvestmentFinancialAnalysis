import re
from typing import Iterator, List, Optional, Tuple

from src.raw_features.consolidated_balance_sheet_rules import (
  CONSOLIDATED_BALANCE_SHEET_RULES,
  METRIC_EXTRACT,
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

      yield company, selected_table
  except Exception as e:
    yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- unexpected exception during Consolidated Balance Sheet read -- {e}"

def extract_metrics(inputs: Iterator[Tuple[str, str]]) -> Iterator[Tuple[str, str]]:
  try:
    from bs4 import BeautifulSoup
    import pandas as pd

    for company, serialized_html_table in inputs:
      soup = BeautifulSoup(serialized_html_table, "html.parser")
      table = soup.find("table")
      if table is None:
        yield company, f"{BALANCE_SHEET_ERR_TEMPLATE} -- failed to parse Consolidated Balance Sheet HTML"
        continue

      metrics = {
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
