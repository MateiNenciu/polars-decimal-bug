#!/usr/bin/env python3

import polars as pl
import adbc_driver_postgresql.dbapi as pg_dbapi
import time
import sys


def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            conn = pg_dbapi.connect(
                "postgresql://testuser:testpass@localhost:5433/decimal_test"
            )
            conn.close()
            print("PostgreSQL is ready!")
            return True
        except Exception:
            retry_count += 1
            print(f"Waiting for PostgreSQL... (attempt {retry_count}/{max_retries})")
            time.sleep(2)

    print("Failed to connect to PostgreSQL after maximum retries")
    return False


def test_adbc_direct_decimal_handling():
    """
    Test decimal handling using ADBC ingest directly (should reproduce the bug)
    """

    print("=== ADBC Direct Decimal Handling Test ===\n")

    # Wait for PostgreSQL
    if not wait_for_postgres():
        sys.exit(1)

    # Step 1: Read from Parquet file
    parquet_file = "test_decimals.parquet"
    try:
        df = pl.read_parquet(parquet_file)
        print("1. Data from Parquet file:")
        print(df)
        print(f"\nSchema: {df.schema}\n")
    except FileNotFoundError:
        print(f"Error: {parquet_file} not found. Run generate_parquet.py first.")
        sys.exit(1)

    # Step 2: Clear existing data and write using direct ADBC ingest
    connection_string = "postgresql://testuser:testpass@localhost:5433/decimal_test"

    try:
        # Clear the table first
        with pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM test_decimals WHERE id > 3")
                conn.commit()

        print("2. Writing to PostgreSQL via direct ADBC ingest...")

        # Use ADBC ingest directly
        with pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                # Direct ingest - ADBC 1.8 supports PyCapsule interface
                n_rows = cursor.adbc_ingest(
                    "test_decimals",
                    data=df,  # Pass Polars DataFrame directly
                    mode="append"
                )
                conn.commit()
                print(f"✓ Ingested {n_rows} rows successfully\n")

    except Exception as e:
        print(f"Error writing to database: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Step 3: Read back and show the bug
    try:
        with pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, decimal_value, description
                    FROM test_decimals
                    WHERE id > 3  -- Only show the data we just inserted
                    ORDER BY id
                """)
                rows = cursor.fetchall()

                print("3. Data from PostgreSQL after direct ADBC write:")
                print("ID | Decimal Value | Description")
                print("-" * 50)
                for row in rows:
                    print(f"{row[0]:2d} | {str(row[1]):13s} | {row[2]}")

                print("\n=== ADBC DIRECT RESULT ===")
                print("Expected: 44.123456 → Actual:", end=" ")

                # Find the specific problematic row
                cursor.execute(
                    "SELECT decimal_value FROM test_decimals WHERE description LIKE '%44.123456%' AND id > 3"
                )
                result = cursor.fetchone()
                if result:
                    actual_value = result[0]
                    print(f"{actual_value}")
                    if str(actual_value) == "44.123456":
                        print("✅ UNEXPECTED SUCCESS: ADBC direct preserved the decimal value!")
                    else:
                        print(f"❌ BUG CONFIRMED: Direct ADBC also converts 44.123456 to {actual_value}")
                        print("   This proves the issue is in the ADBC driver, not Polars.")
                else:
                    print("Could not find the test row")

    except Exception as e:
        print(f"Error reading from database: {e}")


if __name__ == "__main__":
    test_adbc_direct_decimal_handling()