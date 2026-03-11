import re
from typing import Optional

from src.raw_features.constants import RAW_FEATURES

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
  if re.match(r"^total\s+revenues?\s+and\s+other\s+income\b", normalized):
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
    )
  ):
    return False
  if re.match(r"^total\s+revenues?\b", normalized):
    return True
  if re.match(r"^total\s+net\s+sales\b", normalized):
    return True
  if re.match(r"^net\s+sales\b", normalized):
    return True
  if re.match(r"^net\s+revenues?\b", normalized):
    return True
  if re.match(r"^sales\s+revenue\b", normalized):
    return True
  if re.match(r"^revenues?\b", normalized):
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
    return results
  return {}

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

def extract_total_revenue(table) -> Optional[dict[str, int]]:
  total_revenue = _extract_metric_from_rows(table, _is_total_revenue_row)
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
