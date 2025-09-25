#!/usr/bin/env python3

import polars as pl
from decimal import Decimal


def generate_test_parquet():
    """
    Generate a Parquet file with decimal(10,6) data that demonstrates the bug.
    """

    # Create test data with various decimal values
    test_data = [
        {
            "decimal_value": Decimal("44.123456"),
            "description": "Main bug case: 44.123456",
        },
        # {"id": 2, "decimal_value": Decimal("123.456789"), "description": "Another case: 123.456789"},
        # {"id": 3, "decimal_value": Decimal("1.000001"), "description": "Small value: 1.000001"},
        # {"id": 4, "decimal_value": Decimal("99.999999"), "description": "Near max: 99.999999"},
        # {"id": 5, "decimal_value": Decimal("0.000001"), "description": "Minimum precision: 0.000001"},
    ]

    # Create DataFrame with explicit schema matching PostgreSQL table
    df = pl.DataFrame(
        test_data,
        schema={
            "decimal_value": pl.Decimal(precision=10, scale=6),
            "description": pl.Utf8,
        },
    )

    print("Generated DataFrame:")
    print(df)

    print("\nDataFrame schema:")
    print(df.schema)

    # Write to Parquet
    output_file = "test_decimals.parquet"
    df.write_parquet(output_file)
    print(f"\nParquet file written to: {output_file}")

    # Verify by reading back
    df_read = pl.read_parquet(output_file)
    print("\nRead back from Parquet:")
    print(df_read)
    print("\nRead schema:")
    print(df_read.schema)


if __name__ == "__main__":
    generate_test_parquet()
