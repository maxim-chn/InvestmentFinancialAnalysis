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
