# Bank Transactions ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing large-scale bank transaction data using Python, Pandas, and PostgreSQL. This project demonstrates practical data engineering skills for big data analytics and business intelligence.

## 🚀 Project Overview

This ETL pipeline processes a dataset of 2 million bank transaction records, performing data extraction, cleaning, transformation, and loading into a PostgreSQL database with proper schema design and data validation.

### Key Features

- **Scalable Data Processing**: Handles 2M+ records efficiently with batch processing
- **Robust Data Transformation**: Comprehensive data cleaning, validation, and enrichment
- **Database Integration**: PostgreSQL with optimized schema design and indexing
- **Error Handling**: Comprehensive error handling and logging throughout the pipeline
- **Monitoring**: Built-in performance monitoring and reporting
- **Modular Design**: Clean separation of concerns with reusable components

## 📁 Project Structure

```
Big Data Project/
├── main.py                 # Main pipeline orchestration script
├── requirements.txt        # Python dependencies
├── .env.example           # Environment variables template
├── README.md              # This file
├── config/
│   └── database_schema.sql # PostgreSQL database schema
├── src/
│   ├── extract.py         # Data extraction module
│   ├── transform.py       # Data transformation module
│   ├── load.py           # Data loading module
│   └── utils.py          # Utility functions and error handling
├── data/
│   ├── raw/              # Raw data files
│   │   └── 2000000 BT Records.csv
│   └── processed/        # Processed/transformed data
└── logs/                 # Log files and reports
```

## 🛠️ Technology Stack

- **Python 3.8+**: Core programming language
- **Pandas**: Data manipulation and analysis
- **PostgreSQL**: Database for data storage
- **SQLAlchemy**: Database ORM and connection management
- **Psycopg2**: PostgreSQL adapter for Python
- **Python-dotenv**: Environment variable management

## 📋 Prerequisites

1. **Python 3.8 or higher**
2. **PostgreSQL 12 or higher**
3. **Git** (for cloning the repository)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd "Big Data Project"
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Set Up PostgreSQL Database

```bash
# Connect to PostgreSQL as superuser
psql -U postgres

# Create database
CREATE DATABASE etl_database;

# Create user (optional, recommended for production)
CREATE USER etl_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE etl_database TO etl_user;
```

### 4. Configure Environment Variables

```bash
# Copy the environment template
cp .env.example .env

# Edit .env file with your database credentials
```

Update `.env` with your database configuration:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_database
DB_USER=your_username
DB_PASSWORD=your_password
```

### 5. Run the ETL Pipeline

```bash
# Basic execution
python main.py

# With configuration file
python main.py --config config.json

# Generate execution report
python main.py --report --output pipeline_report.json
```

## 📊 Database Schema

The pipeline creates the following tables in PostgreSQL:

### `transaction_categories`
Dimension table for transaction categorization:
- `category_id` (Primary Key)
- `category_name` (Unique)
- `description`
- `created_at`

### `bank_transactions`
Main fact table storing transaction records:
- `transaction_id` (Primary Key)
- `transaction_date`
- `description`
- `transaction_type`
- `deposit_amount`
- `withdrawal_amount`
- `balance_amount`
- `category_id` (Foreign Key)
- `created_at`, `updated_at`

### `transaction_summary`
Aggregated daily summary data:
- `summary_id` (Primary Key)
- `transaction_date` (Unique)
- `total_deposits`
- `total_withdrawals`
- `net_amount`
- `transaction_count`
- `created_at`

## 🔧 Configuration

The pipeline can be configured using a JSON file. Here's an example configuration:

```json
{
  "data_source": {
    "type": "csv",
    "path": "data/raw/2000000 BT Records.csv",
    "batch_size": 100000
  },
  "database": {
    "batch_size": 10000,
    "truncate_tables": false
  },
  "logging": {
    "level": "INFO",
    "file": "logs/etl_pipeline.log"
  },
  "processing": {
    "create_daily_summary": true,
    "validate_data": true,
    "save_intermediate": true
  }
}
```

## 📈 Pipeline Stages

### 1. Extraction
- Reads data from CSV files or URLs
- Supports batch processing of multiple files
- Validates data structure and completeness
- Generates extraction metadata

### 2. Transformation
- **Data Cleaning**: Handles missing values, removes duplicates
- **Date Processing**: Standardizes date formats and extracts temporal features
- **Amount Processing**: Cleans and validates monetary values
- **Categorization**: Automatically categorizes transactions based on descriptions
- **Feature Engineering**: Adds derived columns for analysis
- **Validation**: Ensures data integrity and consistency

### 3. Loading
- Creates database schema automatically
- Loads data in batches for optimal performance
- Updates transaction categories
- Creates daily summaries
- Provides comprehensive loading statistics

## 📊 Performance Metrics

The pipeline tracks and reports:

- **Extraction**: Records processed, extraction rate, file size
- **Transformation**: Records cleaned, transformation rate, data quality metrics
- **Loading**: Load rate, batch performance, database statistics
- **System**: CPU usage, memory consumption, disk I/O

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify PostgreSQL is running
   - Check connection parameters in `.env`
   - Ensure database and user exist

2. **Memory Issues**
   - Reduce batch size in configuration
   - Close other applications
   - Consider using a machine with more RAM

3. **File Not Found Errors**
   - Verify data file paths are correct
   - Check if dataset is downloaded
   - Ensure proper file permissions

### Debug Mode

Enable debug logging by modifying the configuration:

```json
{
  "logging": {
    "level": "DEBUG"
  }
}
```

## 📝 Logging and Monitoring

The pipeline provides comprehensive logging:

- **Console Output**: Real-time progress updates
- **File Logging**: Detailed logs saved to `logs/etl_pipeline.log`
- **Error Reports**: Automatic error report generation
- **Performance Reports**: Detailed execution statistics

## 🧪 Testing

Run individual components for testing:

```bash
# Test extraction
python -c "from src.extract import DataExtractor; e = DataExtractor(); print('Extraction OK')"

# Test transformation
python -c "from src.transform import DataTransformer; t = DataTransformer(); print('Transformation OK')"

# Test database connection
python -c "from src.load import DataLoader; l = DataLoader(); print(l.test_connection())"
```

## 📊 Data Analysis Queries

Once data is loaded, you can run analytical queries:

```sql
-- Daily transaction trends
SELECT 
    transaction_date,
    COUNT(*) as transaction_count,
    SUM(deposit_amount) as total_deposits,
    SUM(withdrawal_amount) as total_withdrawals
FROM bank_transactions
GROUP BY transaction_date
ORDER BY transaction_date;

-- Transaction type analysis
SELECT 
    tc.category_name,
    COUNT(*) as count,
    SUM(bt.deposit_amount) as total_deposits,
    SUM(bt.withdrawal_amount) as total_withdrawals
FROM bank_transactions bt
JOIN transaction_categories tc ON bt.category_id = tc.category_id
GROUP BY tc.category_name
ORDER BY count DESC;
```

## 🔄 Maintenance

### Regular Tasks

1. **Log Rotation**: Clean up old log files periodically
2. **Database Maintenance**: Run VACUUM and ANALYZE on PostgreSQL tables
3. **Backup**: Regular database backups
4. **Monitoring**: Monitor system resources and pipeline performance

### Scaling Considerations

- For larger datasets, consider increasing batch sizes
- Use database partitioning for very large tables
- Implement parallel processing for transformation steps
- Consider using a dedicated database server

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is provided for educational purposes. Please refer to the license file for usage terms.

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the log files for detailed error messages
3. Create an issue in the repository with:
   - Error description
   - Configuration used
   - Relevant log excerpts

## 🎯 Learning Outcomes

This project demonstrates practical skills in:

- **Data Engineering**: Building scalable ETL pipelines
- **Database Design**: Schema design and optimization
- **Data Processing**: Large-scale data manipulation with Pandas
- **Error Handling**: Robust error management and logging
- **Performance Optimization**: Batch processing and resource management
- **Business Intelligence**: Data aggregation and summary generation

Perfect for big data analytics and business intelligence projects! 🚀
