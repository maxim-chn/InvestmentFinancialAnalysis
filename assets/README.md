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
