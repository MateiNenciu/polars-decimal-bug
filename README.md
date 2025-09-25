# Polars Decimal Bug Reproduction

This project reproduces a bug in Polars where decimal values from Parquet files are incorrectly transformed when written to PostgreSQL via the ADBC driver.

## Bug Description

**Expected Behavior:** Decimal value `44.123456` should remain `44.123456` when written to PostgreSQL.

**Actual Behavior:** `numeric field overflow` error occurs when trying to write decimal values to PostgreSQL via ADBC.

**Root Cause:** The bug appears to be a precision/scale mismatch where Polars' `Decimal(10,6)` data is not being correctly interpreted by the PostgreSQL ADBC driver, causing PostgreSQL to reject values that should fit in a `DECIMAL(10,6)` column (which supports values up to 9999.999999).

## System Requirements

- Python 3.12+
- Docker and Docker Compose
- The following Python packages (handled by pyproject.toml):
  - `polars>=1.33.1`
  - `adbc-driver-postgresql>=1.8.0`
  - `pyarrow>=15.0.0`

## Quick Start

1. **Start PostgreSQL:**
   ```bash
   docker-compose up -d
   ```

2. **Install Python dependencies:**
   ```bash
   # Using uv (recommended)
   uv sync

   # Or using pip
   pip install -e .
   ```

3. **Generate test data:**
   ```bash
   uv run generate_parquet.py
   ```

4. **Reproduce the bug using polars write:**
   ```bash
   uv run reproduce_bug.py
   ```

4. **Reproduce the bug using polars write (bug occurs):**
   ```bash
   uv run reproduce_bug.py
   ```

5. **Test with SQLAlchemy (works fine):**
   ```bash
   uv run test_with_sqlalchemy.py
   ```

6. **Test with adbc ingest (bug occurs):**
   ```bash
   uv run test_with_adbc.py
   ```