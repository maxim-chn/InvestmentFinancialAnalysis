import re
from typing import Iterator, Tuple

from src.raw_features.consolidated_cashflow_statements_rules import CONSOLIDATED_BALANCE_SHEET_RULES, METRIC_EXTRACT
from src.raw_features.constants import CASHFLOW_ERR_TEMPLATE, RAW_FEATURES
from src.raw_features.original_filing import extract_tables_from_html, read_original_filing

INTEREST_PAID_FACT_MARKERS = (
  'name="us-gaap:interestpaid"',
  'name="us-gaap:interestpaidnet"',
  "name='us-gaap:interestpaid'",
  "name='us-gaap:interestpaidnet'",
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

def _select_cashflow_table(html_tables: list[str]) -> str | None:
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

def _collect_interest_supplemental_tables(html_text: str, selected_tables: list[str]) -> list[str]:
  from bs4 import BeautifulSoup

  selected_set = set(selected_tables)
  soup = BeautifulSoup(html_text, "html.parser")
  supplemental: list[str] = []
  for table in soup.find_all("table"):
    serialized = str(table)
    if serialized in selected_set:
      continue
    serialized_lower = serialized.lower()
    if any(marker in serialized_lower for marker in INTEREST_PAID_FACT_MARKERS):
      supplemental.append(serialized)
  return supplemental

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

      metrics = {
        RAW_FEATURES.NET_INCOME.value: {},
        RAW_FEATURES.INTEREST_EXPENSE.value: {},
        RAW_FEATURES.TAX_EXPENSE.value: {},
      }
      for table in tables:
        for metric_name, extractor in METRIC_EXTRACT.items():
          metric_values = extractor(table)
          if not metric_values:
            continue
          if not metrics[metric_name]:
            metrics[metric_name] = dict(metric_values)
            continue
          for year, value in metric_values.items():
            if year not in metrics[metric_name]:
              metrics[metric_name][year] = value
      all_years = sorted({
        year
        for metric in metrics.values()
        for year in metric.keys()
      })

      # Keep the pipeline moving when interest expense is not disclosed/extractable.
      interest_metric = metrics[RAW_FEATURES.INTEREST_EXPENSE.value]
      if all_years:
        for year in all_years:
          if year not in interest_metric or interest_metric[year] is None:
            interest_metric[year] = "N/A"

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
