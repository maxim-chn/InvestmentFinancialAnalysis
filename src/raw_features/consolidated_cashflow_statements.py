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
