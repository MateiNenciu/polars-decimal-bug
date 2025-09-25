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

4. **Reproduce the bug:**
   ```bash
   uv run reproduce_bug.py
   ```

## Bug Analysis

The root cause appears to be a precision/scale mismatch in the Polars → ADBC → PostgreSQL pipeline:

1. **Polars DataFrame**: Contains `Decimal(10,6)` values like `44.123456`
2. **ADBC Translation**: Incorrectly interprets or scales these values
3. **PostgreSQL Rejection**: Receives values that exceed `DECIMAL(10,6)` limits (>= 10,000)

This suggests that ADBC is either:
- Sending values without proper decimal scaling
- Misrepresenting the precision/scale metadata
- Converting decimal binary format incorrectly

### Testing Scripts

- `debug_decimal.py` - Deep analysis of decimal conversion
- `test_with_float.py` - Float64 workaround testing
- `test_with_cast.py` - Various casting approaches
- `verify_data.py` - Original verification script

## Files

### Core Files
- `docker-compose.yml` - PostgreSQL 17 container configuration (port 5433)
- `init.sql` - Database initialization script
- `generate_parquet.py` - Creates test Parquet file with decimal data
- `reproduce_bug.py` - Main bug reproduction script
- `pyproject.toml` - Python dependencies

### Analysis & Debug Scripts
- `debug_decimal.py` - Deep analysis of decimal conversion process
- `test_with_float.py` - Tests float64 and string workarounds
- `test_with_cast.py` - Tests various casting approaches
- `verify_data.py` - Data verification and analysis
- `test_connection.py` - Connection testing utility

### Alternative Schemas
- `init_alt.sql` - Additional tables with different decimal precisions for testing

## Environment Information

This reproduction was tested with:
- Python 3.12+
- Polars 1.33.1+
- ADBC PostgreSQL Driver 1.8.0+
- PostgreSQL 17 (via Docker)

## Cleanup

To clean up the environment:

```bash
# Stop and remove PostgreSQL container
docker-compose down -v

# Remove generated files
rm -f test_decimals.parquet
```

## Reporting the Bug

When reporting this bug to the Polars team, include:

1. This reproduction case
2. System information (OS, Python version, package versions)
3. Expected vs actual behavior
4. The fact that it specifically affects decimal precision/scale handling with ADBC