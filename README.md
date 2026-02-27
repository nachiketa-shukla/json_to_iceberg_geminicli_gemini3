# JSON to Iceberg Pipeline

This project provides a robust, well-designed Python pipeline to extract deeply nested JSON data (common in SaaS API responses), handle dynamically inferred schemas via Polars and PyArrow, and write the data natively to an Apache Iceberg table managed by a REST catalog using S3-compatible storage.

## Features

- **Optimal & Minimal Dependencies**: Uses `polars` for exceptionally fast JSON parsing (even with heavy nesting) and `pyiceberg[s3fs]` for native Iceberg interaction.
- **Dynamic Schema Transformation**: Automatically converts all inferred `pa.null()` types (which are not supported in Iceberg) to `pa.string()` recursively through structs and lists.
- **Clean Architecture**: Designed as a reusable OOP pipeline that orchestrates connection configurations, schema cleanups, and catalog operations.
- **Resilient**: Validates namespace existence and automatically attempts to create namespaces or tables if they are absent.

## Installation

```bash
pip install -r requirements.txt
```

## Example Usage

Create an example script (e.g., `main.py`) or use the class directly:

```python
from src.json_to_iceberg import JsonToIcebergPipeline

# 1. Initialize the pipeline
pipeline = JsonToIcebergPipeline(
    catalog_uri="http://localhost:8181",      # Your Iceberg REST catalog URI
    s3_endpoint="http://localhost:9000",      # MinIO/S3 endpoint
    s3_access_key="admin",
    s3_secret_key="password",
    catalog_name="default",
    s3_region="us-east-1"
)

# 2. Process your JSON file
# For normal JSON arrays of objects: is_ndjson=False
# For Newline Delimited JSON (JSONL): is_ndjson=True
pipeline.process_file(
    json_path="saas_response.json",
    table_identifier="my_namespace.my_nested_table",
    is_ndjson=False
)
```

## How It Works

1. **Extraction**: `polars.read_json` efficiently parses the deeply nested JSON file into a DataFrame, fully capturing nested structs and lists.
2. **Schema Scrubbing**: The Polars DataFrame is converted to an Arrow Table. A recursive schema transformation runs over the PyArrow types, intercepting completely empty arrays or completely null fields to promote them to `String` strings so they remain compatible with Iceberg’s strict type system.
3. **Registration & Write**: The `pyiceberg` client creates (or loads) a table within the REST catalog and appends the PyArrow table to it. The write operation directly outputs Parquet files to the configured S3 bucket and commits the new snapshot metadata to the catalog.
