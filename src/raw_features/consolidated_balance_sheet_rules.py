from __future__ import annotations

import re
from typing import Optional
from src.raw_features.constants import RAW_FEATURES

CURRENT_ASSETS_KEY = RAW_FEATURES.CURRENT_ASSETS.value
CURRENT_LIABILITIES_KEY = RAW_FEATURES.CURRENT_LIABILITIES.value
TOTAL_ASSETS_KEY = RAW_FEATURES.TOTAL_ASSETS.value
LONG_TERM_DEBT_KEY = RAW_FEATURES.LONG_TERM_DEBT.value
SHORT_TERM_DEBT_KEY = RAW_FEATURES.SHORT_TERM_DEBT.value
STOCKHOLDERS_EQUITY_KEY = RAW_FEATURES.STOCKHOLDERS_EQUITY.value

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

def _has_liabilities_equity_marker(table_text: str) -> bool:
  if any(marker in table_text for marker in LIABILITIES_EQUITY_MARKERS):
    return True
  return (
    "liabilities" in table_text
    and any(term in table_text for term in EQUITY_TERMS)
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

def extract_units(table) -> dict[str, str]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
    return {}

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

  unit = _detect_units(" ".join(candidates))
  if unit is None:
    return {}

  years = sorted(set(year_by_col.values()))
  return {year: unit for year in years}


def extract_current_assets(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
    return {}
  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()
    if "total current assets" not in row_text:
      continue
    results: dict[str, int] = {}
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan
    return results
  return {}

def extract_current_liabilities(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
    return {}
  for row in table.find_all("tr"):
    row_text = row.get_text(" ", strip=True).lower()
    if "total current liabilities" not in row_text:
      continue
    results: dict[str, int] = {}
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan
    return results
  return {}

def extract_total_assets(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
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
    "liabilities and stockholders' equity",
    "liabilities and stockholders equity",
    "liabilities and shareholders' equity",
    "liabilities and shareholders equity",
    "liabilities and stockholders' deficit",
    "liabilities and stockholders deficit",
    "liabilities and shareholders' deficit",
    "liabilities and shareholders deficit",
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
    row_html = str(row).lower()
    has_total_assets_xbrl = any(
      f'name="{concept}"' in row_html or f"name='{concept}'" in row_html
      for concept in total_assets_xbrl_concepts
    )
    has_total_assets_marker = (
      "total assets" in row_text
      or any(marker in row_text for marker in total_assets_fallback_markers)
    )
    candidate_by_split_layout = awaiting_total_assets_values and _is_unlabeled_numeric_total_row(row_text)
    if (
      not has_total_assets_marker
      and not has_total_assets_xbrl
      and not candidate_by_split_layout
    ):
      continue
    results: dict[str, int] = {}
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan
    if results:
      return results
    if has_total_assets_marker or has_total_assets_xbrl:
      awaiting_total_assets_values = True
  return {}

def _is_stockholders_equity_row(row_text: str) -> bool:
  row_text = _normalize_metric_text(row_text)
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
  )
  if any(marker in row_text for marker in primary_markers):
    return True

  if "total equity" in row_text:
    return not any(
      marker in row_text
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
  if not year_by_col:
    return {}
  for row in table.find_all("tr"):
    row_text = _normalize_metric_text(row.get_text(" ", strip=True))
    if not _is_stockholders_equity_row(row_text):
      continue
    results: dict[str, int] = {}
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan
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
  if not year_by_col:
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
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan

    if not results:
      continue

    if best_score is None or score > best_score or (score == best_score and section in ("noncurrent", "after_current")):
      best_score = score
      best_results = results

  if best_results is not None:
    return best_results

  if not long_term_debt_like_in_liabilities:
    years = sorted(set(year_by_col.values()))
    return {year: 0 for year in years}

  return {}

def extract_short_term_debt(table) -> dict[str, int]:
  year_by_col = _year_by_column(table)
  if not year_by_col:
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
    col_idx = 0
    for cell in row.find_all(["td", "th"]):
      colspan = int(cell.get("colspan", 1))
      value = _parse_number(cell.get_text(" ", strip=True))
      if value is not None:
        for offset in range(colspan):
          year = year_by_col.get(col_idx + offset)
          if year and year not in results:
            results[year] = value
      col_idx += colspan

    if not results:
      continue

    if best_score is None or score > best_score or (score == best_score and section == "current"):
      best_score = score
      best_results = results

  if best_results is not None:
    return best_results

  if not debt_like_in_liabilities:
    years = sorted(set(year_by_col.values()))
    return {year: 0 for year in years}

  return {}

CONSOLIDATED_BALANCE_SHEET_RULES = [has_mandatory_metrics]

METRIC_EXTRACT = {
  CURRENT_ASSETS_KEY: extract_current_assets,
  CURRENT_LIABILITIES_KEY: extract_current_liabilities,
  TOTAL_ASSETS_KEY: extract_total_assets,
  LONG_TERM_DEBT_KEY: extract_long_term_debt,
  SHORT_TERM_DEBT_KEY: extract_short_term_debt,
  STOCKHOLDERS_EQUITY_KEY: extract_stockholders_equity,
}
