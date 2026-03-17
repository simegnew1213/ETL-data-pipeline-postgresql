"""
Data Loading Module for Bank Transactions ETL Pipeline
Handles loading transformed data into PostgreSQL database
"""

import pandas as pd
import psycopg2
from psycopg2 import sql, extras
import sqlalchemy
from sqlalchemy import create_engine, text
import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime
from dotenv import load_dotenv
import time
from urllib.parse import quote_plus


class DataLoader:
    """Class responsible for loading data into PostgreSQL database."""
    
    def __init__(self, logger: Optional[logging.Logger] = None, 
                 connection_string: Optional[str] = None):
        """
        Initialize the DataLoader.
        
        Args:
            logger: Logger instance
            connection_string: Database connection string
        """
        self.logger = logger or self._setup_logger()
        self.connection_string = connection_string or self._get_connection_string()
        self.engine = None
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _get_connection_string(self) -> str:
        """Get database connection string from environment variables."""
        load_dotenv()
        
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'etl_database')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'password')

        # URL-encode credentials to support special characters (e.g. @, :, /)
        safe_user = quote_plus(str(db_user))
        safe_password = quote_plus(str(db_password))

        return f"postgresql://{safe_user}:{safe_password}@{db_host}:{db_port}/{db_name}"
    
    def create_engine(self) -> sqlalchemy.engine.Engine:
        """Create SQLAlchemy engine."""
        try:
            if not self.engine:
                self.engine = create_engine(
                    self.connection_string,
                    pool_size=10,
                    max_overflow=20,
                    pool_pre_ping=True,
                    echo=False
                )
                self.logger.info("Database engine created successfully")
            return self.engine
        except Exception as e:
            self.logger.error(f"Error creating database engine: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            engine = self.create_engine()
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                self.logger.info("Database connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def execute_schema(self, schema_file_path: str) -> None:
        """
        Execute database schema from SQL file.
        
        Args:
            schema_file_path: Path to the SQL schema file
        """
        try:
            self.logger.info(f"Executing database schema from {schema_file_path}")
            
            if not os.path.exists(schema_file_path):
                raise FileNotFoundError(f"Schema file not found: {schema_file_path}")
            
            # Read SQL file
            with open(schema_file_path, 'r', encoding='utf-8') as file:
                schema_sql = file.read()

            # Execute the full SQL script using a raw psycopg2 connection.
            # This avoids breaking function definitions that use $$ ... $$ blocks.
            engine = self.create_engine()
            raw_conn = engine.raw_connection()
            try:
                raw_conn.autocommit = True
                with raw_conn.cursor() as cur:
                    cur.execute(schema_sql)
            finally:
                raw_conn.close()
            
            self.logger.info("Database schema executed successfully")
            
        except Exception as e:
            self.logger.error(f"Error executing database schema: {str(e)}")
            raise
    
    def load_transactions(self, df: pd.DataFrame, 
                         batch_size: int = 10000,
                         truncate_table: bool = False) -> Dict[str, Any]:
        """
        Load bank transactions data into the database.
        
        Args:
            df: DataFrame containing transaction data
            batch_size: Number of records to insert in each batch
            truncate_table: Whether to truncate table before loading
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            self.logger.info(f"Starting to load {len(df)} transactions")
            
            # Validate required columns
            required_columns = ['transaction_date', 'description', 'deposit_amount', 
                              'withdrawal_amount', 'balance_amount', 'transaction_type']
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            engine = self.create_engine()
            
            # Truncate table if requested
            if truncate_table:
                with engine.connect() as connection:
                    connection.execute(text("TRUNCATE TABLE bank_transactions RESTART IDENTITY CASCADE"))
                    connection.commit()
                self.logger.info("Bank transactions table truncated")
            
            # Prepare data for loading
            df_load = df[required_columns].copy()
            df_load['created_at'] = datetime.now()
            df_load['updated_at'] = datetime.now()
            
            # Load data in batches
            total_loaded = 0
            start_time = time.time()
            
            for i in range(0, len(df_load), batch_size):
                batch_df = df_load.iloc[i:i+batch_size]
                
                with engine.connect() as connection:
                    batch_df.to_sql(
                        'bank_transactions',
                        connection,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=batch_size
                    )
                    connection.commit()
                
                total_loaded += len(batch_df)
                self.logger.info(f"Loaded {total_loaded}/{len(df)} records")
            
            # Update category IDs based on transaction types
            self._update_transaction_categories(engine)
            
            load_time = time.time() - start_time
            
            stats = {
                'total_records': len(df),
                'loaded_records': total_loaded,
                'load_time_seconds': round(load_time, 2),
                'records_per_second': round(total_loaded / load_time, 2) if load_time > 0 else 0
            }
            
            self.logger.info(f"Transaction loading completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error loading transactions: {str(e)}")
            raise
    
    def _update_transaction_categories(self, engine: sqlalchemy.engine.Engine) -> None:
        """Update category IDs based on transaction types."""
        try:
            with engine.connect() as connection:
                update_query = text("""
                    UPDATE bank_transactions 
                    SET category_id = tc.category_id
                    FROM transaction_categories tc
                    WHERE bank_transactions.transaction_type = tc.category_name
                """)
                connection.execute(update_query)
                connection.commit()
                
            self.logger.info("Transaction categories updated successfully")
            
        except Exception as e:
            self.logger.warning(f"Error updating transaction categories: {str(e)}")
    
    def load_daily_summary(self, df: pd.DataFrame,
                          truncate_table: bool = False) -> Dict[str, Any]:
        """
        Load daily summary data into the database.
        
        Args:
            df: DataFrame containing daily summary data
            truncate_table: Whether to truncate table before loading
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            self.logger.info(f"Starting to load daily summary with {len(df)} records")
            
            # Validate required columns
            required_columns = ['transaction_date', 'total_deposits', 'total_withdrawals',
                              'net_amount', 'transaction_count']
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            engine = self.create_engine()
            
            # Truncate table if requested
            if truncate_table:
                with engine.connect() as connection:
                    connection.execute(text("TRUNCATE TABLE transaction_summary RESTART IDENTITY CASCADE"))
                    connection.commit()
                self.logger.info("Transaction summary table truncated")
            
            # Prepare data for loading
            df_load = df[required_columns].copy()
            df_load['created_at'] = datetime.now()
            
            # Load data
            start_time = time.time()
            
            with engine.connect() as connection:
                df_load.to_sql(
                    'transaction_summary',
                    connection,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                connection.commit()
            
            load_time = time.time() - start_time
            
            stats = {
                'total_records': len(df),
                'loaded_records': len(df),
                'load_time_seconds': round(load_time, 2),
                'records_per_second': round(len(df) / load_time, 2) if load_time > 0 else 0
            }
            
            self.logger.info(f"Daily summary loading completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error loading daily summary: {str(e)}")
            raise
    
    def upsert_daily_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Upsert daily summary data (insert or update).
        
        Args:
            df: DataFrame containing daily summary data
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            self.logger.info(f"Starting upsert of daily summary with {len(df)} records")
            
            engine = self.create_engine()
            
            # Prepare data
            df_load = df.copy()
            df_load['created_at'] = datetime.now()
            
            # Upsert records
            start_time = time.time()
            updated_count = 0
            inserted_count = 0
            
            with engine.connect() as connection:
                for _, row in df_load.iterrows():
                    # Check if record exists
                    check_query = text("""
                        SELECT COUNT(*) FROM transaction_summary 
                        WHERE transaction_date = :date
                    """)
                    result = connection.execute(check_query, {'date': row['transaction_date']})
                    exists = result.scalar() > 0
                    
                    if exists:
                        # Update existing record
                        update_query = text("""
                            UPDATE transaction_summary SET
                                total_deposits = :total_deposits,
                                total_withdrawals = :total_withdrawals,
                                net_amount = :net_amount,
                                transaction_count = :transaction_count
                            WHERE transaction_date = :date
                        """)
                        connection.execute(update_query, {
                            'total_deposits': row['total_deposits'],
                            'total_withdrawals': row['total_withdrawals'],
                            'net_amount': row['net_amount'],
                            'transaction_count': row['transaction_count'],
                            'date': row['transaction_date']
                        })
                        updated_count += 1
                    else:
                        # Insert new record
                        row_dict = row.to_dict()
                        connection.execute(text("""
                            INSERT INTO transaction_summary 
                            (transaction_date, total_deposits, total_withdrawals, 
                             net_amount, transaction_count, created_at)
                            VALUES (:transaction_date, :total_deposits, :total_withdrawals,
                                   :net_amount, :transaction_count, :created_at)
                        """), row_dict)
                        inserted_count += 1
                
                connection.commit()
            
            load_time = time.time() - start_time
            
            stats = {
                'total_records': len(df),
                'updated_records': updated_count,
                'inserted_records': inserted_count,
                'load_time_seconds': round(load_time, 2)
            }
            
            self.logger.info(f"Daily summary upsert completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error upserting daily summary: {str(e)}")
            raise
    
    def get_load_statistics(self) -> Dict[str, Any]:
        """Get statistics about loaded data."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                # Get transaction count
                trans_count = connection.execute(text("SELECT COUNT(*) FROM bank_transactions")).scalar()
                
                # Get summary count
                summary_count = connection.execute(text("SELECT COUNT(*) FROM transaction_summary")).scalar()
                
                # Get date range
                date_range = connection.execute(text("""
                    SELECT 
                        MIN(transaction_date) as min_date,
                        MAX(transaction_date) as max_date
                    FROM bank_transactions
                """)).fetchone()
                
                # Get total amounts
                amounts = connection.execute(text("""
                    SELECT 
                        SUM(deposit_amount) as total_deposits,
                        SUM(withdrawal_amount) as total_withdrawals,
                        SUM(deposit_amount - withdrawal_amount) as net_amount
                    FROM bank_transactions
                """)).fetchone()
            
            stats = {
                'transaction_count': trans_count,
                'summary_count': summary_count,
                'date_range': {
                    'start': date_range[0].isoformat() if date_range[0] else None,
                    'end': date_range[1].isoformat() if date_range[1] else None
                },
                'total_amounts': {
                    'deposits': float(amounts[0]) if amounts[0] else 0,
                    'withdrawals': float(amounts[1]) if amounts[1] else 0,
                    'net': float(amounts[2]) if amounts[2] else 0
                }
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting load statistics: {str(e)}")
            return {}
    
    def close_connections(self):
        """Close database connections."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connections closed")


# Example usage and testing
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create loader instance
    loader = DataLoader()
    
    try:
        # Test connection
        if loader.test_connection():
            print("Database connection successful!")
            
            # Execute schema
            loader.execute_schema("../config/database_schema.sql")
            print("Schema executed successfully!")
            
            # Get statistics
            stats = loader.get_load_statistics()
            print(f"Current database statistics: {stats}")
        else:
            print("Database connection failed!")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        loader.close_connections()
