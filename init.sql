-- Create the test table for decimal bug reproduction
CREATE TABLE IF NOT EXISTS test_decimals (
    id SERIAL PRIMARY KEY,
    decimal_value DECIMAL(10,6) NOT NULL,
    description TEXT
);

-- Insert some initial test data to verify table structure
INSERT INTO test_decimals (decimal_value, description) VALUES
    (44.123456, 'Expected: 44.123456'),
    (123.456789, 'Expected: 123.456789'),
    (1.000001, 'Expected: 1.000001');

-- Create an index on the decimal value for performance
CREATE INDEX IF NOT EXISTS idx_decimal_value ON test_decimals(decimal_value);