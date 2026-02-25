import os
from enum import Enum

BALANCE_SHEET_ERR_TEMPLATE = "failed to process balance sheet"
CASHFLOW_ERR_TEMPLATE = "failed to process cash flow statement"
BASE_DIR = os.path.join("/home", "zonenp", "final_project")
FILINGS_DIR = os.path.join(BASE_DIR, "assets", "filings_10k")
RAW_TABLES_DIR = os.path.join(BASE_DIR, "assets", "raw_html_tables")
TARGET_YEAR_PREFIX_TEMPLATE = "filing-%s-"

class RAW_FEATURES(Enum):
  CURRENT_ASSETS = "current_assets"
  CURRENT_LIABILITIES = "current_liabilities"
  LONG_TERM_DEBT = "long_term_debt"
  INTEREST_EXPENSE = "interest_expense"
  NET_INCOME = "net_income"
  SHORT_TERM_DEBT = "short_term_debt"
  STOCKHOLDERS_EQUITY = "stockholders_equity"
  TAX_EXPENSE = "tax_expense"
  TOTAL_ASSETS = "total_assets"
  TOTAL_REVENUE = "total_revenue"
  UNITS = "units"
