-- Database Schema for Bank Transactions ETL Pipeline
-- PostgreSQL Schema Design

-- Create database (if needed)
-- CREATE DATABASE etl_database;

-- Connect to the database
-- \c etl_database;

-- Drop existing tables (for clean re-runs)
DROP TABLE IF EXISTS bank_transactions CASCADE;
DROP TABLE IF EXISTS transaction_categories CASCADE;
DROP TABLE IF EXISTS transaction_summary CASCADE;

-- Create transaction categories table (dimension table)
CREATE TABLE transaction_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create main bank transactions table (fact table)
CREATE TABLE bank_transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    description VARCHAR(255) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    deposit_amount DECIMAL(15,2) DEFAULT 0.00,
    withdrawal_amount DECIMAL(15,2) DEFAULT 0.00,
    balance_amount DECIMAL(15,2) NOT NULL,
    category_id INTEGER REFERENCES transaction_categories(category_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create transaction summary table (aggregated data)
CREATE TABLE transaction_summary (
    summary_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    total_deposits DECIMAL(15,2) DEFAULT 0.00,
    total_withdrawals DECIMAL(15,2) DEFAULT 0.00,
    net_amount DECIMAL(15,2) NOT NULL,
    transaction_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_date)
);

-- Create indexes for better query performance
CREATE INDEX idx_bank_transactions_date ON bank_transactions(transaction_date);
CREATE INDEX idx_bank_transactions_type ON bank_transactions(transaction_type);
CREATE INDEX idx_bank_transactions_category ON bank_transactions(category_id);
CREATE INDEX idx_transaction_summary_date ON transaction_summary(transaction_date);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_bank_transactions_updated_at 
    BEFORE UPDATE ON bank_transactions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default transaction categories
INSERT INTO transaction_categories (category_name, description) VALUES
('Transfer', 'Fund transfer (NEFT/RTGS/IMPS etc.)'),
('Reversal', 'Transaction reversal or refund'),
('Debit Card', 'Debit card transaction'),
('Purchase', 'Purchase transaction'),
('ATM', 'ATM withdrawal or deposit'),
('Tax', 'Tax payment or refund'),
('Miscellaneous', 'Other transactions not categorized'),
('Payment', 'Bill payment or other payment'),
('Credit', 'Credit transaction');

-- Grant permissions (adjust username as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;
