#!/usr/bin/env python3

import polars as pl
import pandas as pd
import sqlalchemy as sa
import time
import sys
from decimal import Decimal


def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            engine = sa.create_engine(
                "postgresql://testuser:testpass@localhost:5433/decimal_test"
            )
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            print("PostgreSQL is ready!")
            return True
        except Exception:
            retry_count += 1
            print(f"Waiting for PostgreSQL... (attempt {retry_count}/{max_retries})")
            time.sleep(2)

    print("Failed to connect to PostgreSQL after maximum retries")
    return False


def test_sqlalchemy_decimal_handling():
    """
    Test decimal handling using SQLAlchemy (should work correctly)
    """

    print("=== SQLAlchemy Decimal Handling Test ===\n")

    # Wait for PostgreSQL
    if not wait_for_postgres():
        sys.exit(1)

    # Step 1: Read from Parquet file
    parquet_file = "test_decimals.parquet"
    try:
        df_polars = pl.read_parquet(parquet_file)
        print("1. Data from Parquet file:")
        print(df_polars)
        print(f"\nSchema: {df_polars.schema}\n")

        # Convert to pandas for SQLAlchemy
        df_pandas = df_polars.to_pandas()
        print("1a. Converted to pandas:")
        print(df_pandas)
        print(f"pandas dtypes: {df_pandas.dtypes}\n")

    except FileNotFoundError:
        print(f"Error: {parquet_file} not found. Run generate_parquet.py first.")
        sys.exit(1)

    # Step 2: Clear existing data and write to PostgreSQL using SQLAlchemy
    engine = sa.create_engine(
        "postgresql://testuser:testpass@localhost:5433/decimal_test"
    )

    try:
        print("2. Writing to PostgreSQL via SQLAlchemy...")

        # Write using pandas + SQLAlchemy
        df_pandas.to_sql(
            name="test_decimals",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        print("✓ Data written successfully\n")

    except Exception as e:
        print(f"Error writing to database: {e}")
        sys.exit(1)

    # Step 3: Read back and show the results
    try:
        with engine.connect() as conn:
            # First, show all rows to understand the data
            result = conn.execute(
                sa.text("""
                SELECT id, decimal_value, description
                FROM test_decimals
                ORDER BY id
            """)
            )
            all_rows = result.fetchall()

            print("3a. ALL data in PostgreSQL table:")
            print("ID | Decimal Value | Description")
            print("-" * 50)
            for row in all_rows:
                print(f"{row[0]:2d} | {str(row[1]):13s} | {row[2]}")

            # Now show just the newly inserted data
            result = conn.execute(
                sa.text("""
                SELECT id, decimal_value, description
                FROM test_decimals
                WHERE id > 3  -- Only show the data we just inserted
                ORDER BY id
            """)
            )
            rows = result.fetchall()

            print("\n3b. Data from PostgreSQL after SQLAlchemy write (ID > 3):")
            print("ID | Decimal Value | Description")
            print("-" * 50)
            for row in rows:
                print(f"{row[0]:2d} | {str(row[1]):13s} | {row[2]}")

            print("\n=== SQLAlchemy RESULT ===")
            print("Expected: 44.123456 → Actual:", end=" ")

            # Find the specific problematic row
            result = conn.execute(
                sa.text(
                    "SELECT decimal_value FROM test_decimals WHERE description LIKE '%44.123456%' AND id > 3"
                )
            )
            row = result.fetchone()
            if row:
                actual_value = row[0]
                print(f"{actual_value}")
                if str(actual_value) == "44.123456":
                    print(
                        "✅ SUCCESS: SQLAlchemy preserved the decimal value correctly!"
                    )
                else:
                    print(f"❌ UNEXPECTED: 44.123456 became {actual_value}")
            else:
                print("Could not find the test row")

    except Exception as e:
        print(f"Error reading from database: {e}")


if __name__ == "__main__":
    test_sqlalchemy_decimal_handling()
