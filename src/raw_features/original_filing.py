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
