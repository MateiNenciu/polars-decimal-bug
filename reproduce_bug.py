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


def reproduce_decimal_bug():
    """
    Reproduce the decimal bug by:
    1. Reading decimal data from Parquet file
    2. Writing to PostgreSQL using ADBC
    3. Showing the incorrect transformation
    """

    print("=== Polars Decimal Bug Reproduction ===\n")

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

    # Step 2: Clear existing data and write to PostgreSQL
    connection_string = "postgresql://testuser:testpass@localhost:5433/decimal_test"

    try:
        # Clear the table first (except initial test data)
        with pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM test_decimals WHERE id > 3")
                conn.commit()

        print("2. Writing to PostgreSQL via ADBC...")

        # Write using Polars ADBC integration
        df.write_database(
            table_name="test_decimals",
            connection="postgresql://testuser:testpass@localhost:5433/decimal_test",
            engine="adbc",
            if_table_exists="append",
        )

        print("✓ Data written successfully\n")

    except Exception as e:
        print(f"Error writing to database: {e}")
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

                print("3. Data from PostgreSQL after writing:")
                print("ID | Decimal Value | Description")
                print("-" * 50)
                for row in rows:
                    print(f"{row[0]:2d} | {row[1]:13s} | {row[2]}")

                print("\n=== BUG DEMONSTRATION ===")
                print("Expected: 44.123456 → Actual:", end=" ")

                # Find the specific problematic row
                cursor.execute(
                    "SELECT decimal_value FROM test_decimals WHERE description LIKE '%44.123456%' AND id > 3"
                )
                result = cursor.fetchone()
                if result:
                    actual_value = result[0]
                    print(f"{actual_value}")
                    print(f"❌ BUG CONFIRMED: 44.123456 became {actual_value}")
                else:
                    print("Could not find the test row")

    except Exception as e:
        print(f"Error reading from database: {e}")


if __name__ == "__main__":
    reproduce_decimal_bug()
