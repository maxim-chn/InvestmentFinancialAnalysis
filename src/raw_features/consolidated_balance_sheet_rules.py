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
