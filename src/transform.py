"""
Data Transformation Module for Bank Transactions ETL Pipeline
Handles data cleaning, validation, and transformation operations
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from decimal import Decimal, InvalidOperation


class DataTransformer:
    """Class responsible for transforming and cleaning data."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the DataTransformer."""
        self.logger = logger or self._setup_logger()
        
        # Transaction type mapping
        self.transaction_type_mapping = {
            'NEFT': 'Transfer',
            'Reversal': 'Reversal',
            'Debit Card': 'Debit Card',
            'Purchase': 'Purchase',
            'ATM': 'ATM',
            'Tax': 'Tax',
            'Miscellaneous': 'Miscellaneous',
            'Transfer': 'Transfer',
            'Payment': 'Payment',
            'Credit': 'Credit'
        }
        
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
    
    def clean_bank_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform bank transactions data.
        
        Args:
            df: Raw bank transactions DataFrame
            
        Returns:
            Cleaned and transformed DataFrame
        """
        try:
            self.logger.info("Starting bank transactions data transformation")
            df_clean = df.copy()
            
            # 1. Clean column names
            df_clean = self._clean_column_names(df_clean)
            
            # 2. Handle missing values
            df_clean = self._handle_missing_values(df_clean)
            
            # 3. Clean and standardize dates
            df_clean = self._clean_dates(df_clean)
            
            # 4. Clean and standardize amounts
            df_clean = self._clean_amounts(df_clean)
            
            # 5. Clean descriptions and categorize transactions
            df_clean = self._clean_descriptions(df_clean)
            
            # 6. Add derived columns
            df_clean = self._add_derived_columns(df_clean)
            
            # 7. Remove duplicates
            df_clean = self._remove_duplicates(df_clean)
            
            # 8. Validate data integrity
            self._validate_transformed_data(df_clean)
            
            self.logger.info(f"Transformation completed. Final shape: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error in data transformation: {str(e)}")
            raise
    
    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize column names."""
        column_mapping = {
            'Date': 'transaction_date',
            'Description': 'description',
            'Deposits': 'deposit_amount',
            'Withdrawls': 'withdrawal_amount',  # Note: typo in original data
            'Balance': 'balance_amount'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert to lowercase and replace spaces with underscores
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        
        self.logger.info(f"Column names cleaned: {list(df.columns)}")
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset."""
        # Check for missing values
        missing_counts = df.isnull().sum()
        if missing_counts.sum() > 0:
            self.logger.warning(f"Missing values found: {missing_counts[missing_counts > 0].to_dict()}")
        
        # Fill missing deposits and withdrawals with 0
        if 'deposit_amount' in df.columns:
            df['deposit_amount'] = df['deposit_amount'].fillna(0)
        if 'withdrawal_amount' in df.columns:
            df['withdrawal_amount'] = df['withdrawal_amount'].fillna(0)
        
        # Drop rows with missing critical fields
        critical_fields = ['transaction_date', 'description', 'balance_amount']
        df = df.dropna(subset=critical_fields)
        
        return df
    
    def _clean_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize date columns."""
        if 'transaction_date' not in df.columns:
            return df
        
        # Convert to datetime, handling various date formats
        try:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='mixed')
        except:
            # Try alternative formats
            date_formats = ['%d-%b-%Y', '%d-%B-%Y', '%Y-%m-%d', '%m/%d/%Y']
            for fmt in date_formats:
                try:
                    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format=fmt)
                    break
                except:
                    continue
        
        # Extract date components
        df['year'] = df['transaction_date'].dt.year
        df['month'] = df['transaction_date'].dt.month
        df['day'] = df['transaction_date'].dt.day
        df['day_of_week'] = df['transaction_date'].dt.dayofweek
        df['quarter'] = df['transaction_date'].dt.quarter
        
        self.logger.info("Date cleaning completed")
        return df
    
    def _clean_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize amount columns."""
        amount_columns = ['deposit_amount', 'withdrawal_amount', 'balance_amount']
        
        for col in amount_columns:
            if col in df.columns:
                # Remove commas and convert to numeric
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('"', '')
                
                # Convert to decimal for precision
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    # Handle any remaining formatting issues
                    df[col] = df[col].apply(lambda x: self._clean_amount_value(x))
                
                # Fill any remaining NaN values with 0 (except balance)
                if col != 'balance_amount':
                    df[col] = df[col].fillna(0)
        
        self.logger.info("Amount cleaning completed")
        return df
    
    def _clean_amount_value(self, value) -> float:
        """Clean individual amount values."""
        if pd.isna(value) or value == '':
            return 0.0
        
        try:
            # Remove any non-numeric characters except decimal point
            cleaned = re.sub(r'[^\d.-]', '', str(value))
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0
    
    def _clean_descriptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean transaction descriptions and categorize transactions."""
        if 'description' not in df.columns:
            return df
        
        # Clean description text
        df['description'] = df['description'].astype(str).str.strip()
        df['description'] = df['description'].str.title()
        
        # Categorize transactions based on description
        df['transaction_type'] = df['description'].apply(self._categorize_transaction)
        
        self.logger.info("Description cleaning and categorization completed")
        return df
    
    def _categorize_transaction(self, description: str) -> str:
        """Categorize transaction based on description."""
        if pd.isna(description):
            return 'Miscellaneous'
        
        description_lower = description.lower()
        
        # Check for known transaction types
        for keyword, category in self.transaction_type_mapping.items():
            if keyword.lower() in description_lower:
                return category
        
        # Additional pattern matching
        if any(word in description_lower for word in ['transfer', 'neft', 'rtgs', 'imps']):
            return 'Transfer'
        elif any(word in description_lower for word in ['card', 'pos', 'swipe']):
            return 'Debit Card'
        elif any(word in description_lower for word in ['atm', 'cash']):
            return 'ATM'
        elif any(word in description_lower for word in ['tax', 'gst', 'tds']):
            return 'Tax'
        elif any(word in description_lower for word in ['refund', 'reverse']):
            return 'Reversal'
        elif any(word in description_lower for word in ['purchase', 'shop', 'store']):
            return 'Purchase'
        elif any(word in description_lower for word in ['payment', 'bill', 'emi']):
            return 'Payment'
        elif any(word in description_lower for word in ['credit', 'deposit', 'salary']):
            return 'Credit'
        else:
            return 'Miscellaneous'
    
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns for analysis."""
        # Calculate net transaction amount
        if 'deposit_amount' in df.columns and 'withdrawal_amount' in df.columns:
            df['net_amount'] = df['deposit_amount'] - df['withdrawal_amount']
        
        # Add transaction amount category
        if 'net_amount' in df.columns:
            df['amount_category'] = pd.cut(
                df['net_amount'].abs(),
                bins=[0, 100, 1000, 10000, float('inf')],
                labels=['Small', 'Medium', 'Large', 'Very Large']
            )
        
        # Add weekend indicator
        if 'day_of_week' in df.columns:
            df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        # Add month name
        if 'month' in df.columns:
            df['month_name'] = df['month'].apply(lambda x: datetime(2020, x, 1).strftime('%B'))
        
        self.logger.info("Derived columns added")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate transactions."""
        initial_count = len(df)
        
        # Define columns to check for duplicates
        duplicate_columns = ['transaction_date', 'description', 'deposit_amount', 
                           'withdrawal_amount', 'balance_amount']
        available_columns = [col for col in duplicate_columns if col in df.columns]
        
        if available_columns:
            df = df.drop_duplicates(subset=available_columns, keep='first')
        
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate transactions")
        
        return df
    
    def _validate_transformed_data(self, df: pd.DataFrame) -> None:
        """Validate the transformed data."""
        validation_errors = []
        
        # Check for negative balance (shouldn't happen in normal banking)
        if 'balance_amount' in df.columns:
            negative_balances = (df['balance_amount'] < 0).sum()
            if negative_balances > 0:
                validation_errors.append(f"Found {negative_balances} records with negative balance")
        
        # Check for invalid dates
        if 'transaction_date' in df.columns:
            future_dates = (df['transaction_date'] > datetime.now()).sum()
            if future_dates > 0:
                validation_errors.append(f"Found {future_dates} records with future dates")
        
        # Check for amount consistency
        if all(col in df.columns for col in ['deposit_amount', 'withdrawal_amount', 'balance_amount']):
            # Both deposit and withdrawal shouldn't be positive for same transaction
            both_positive = ((df['deposit_amount'] > 0) & (df['withdrawal_amount'] > 0)).sum()
            if both_positive > 0:
                validation_errors.append(f"Found {both_positive} records with both deposit and withdrawal")
        
        if validation_errors:
            for error in validation_errors:
                self.logger.warning(f"Data validation warning: {error}")
        else:
            self.logger.info("Data validation passed")
    
    def aggregate_daily_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create daily transaction summary.
        
        Args:
            df: Transformed transactions DataFrame
            
        Returns:
            Daily summary DataFrame
        """
        try:
            if 'transaction_date' not in df.columns:
                raise ValueError("transaction_date column required for aggregation")
            
            # Group by date and aggregate
            daily_summary = df.groupby('transaction_date').agg({
                'deposit_amount': 'sum',
                'withdrawal_amount': 'sum',
                'balance_amount': 'last',  # Use last balance of the day
                'description': 'count'  # Transaction count
            }).reset_index()
            
            # Rename columns
            daily_summary.columns = ['transaction_date', 'total_deposits', 
                                   'total_withdrawals', 'ending_balance', 'transaction_count']
            
            # Calculate net amount
            daily_summary['net_amount'] = daily_summary['total_deposits'] - daily_summary['total_withdrawals']
            
            # Sort by date
            daily_summary = daily_summary.sort_values('transaction_date')
            
            self.logger.info(f"Created daily summary with {len(daily_summary)} records")
            return daily_summary
            
        except Exception as e:
            self.logger.error(f"Error creating daily summary: {str(e)}")
            raise
    
    def get_transformation_metadata(self, df_original: pd.DataFrame, 
                                  df_transformed: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate metadata about the transformation process.
        
        Args:
            df_original: Original DataFrame
            df_transformed: Transformed DataFrame
            
        Returns:
            Dictionary containing transformation metadata
        """
        metadata = {
            'transformation_timestamp': datetime.now().isoformat(),
            'original_shape': df_original.shape,
            'transformed_shape': df_transformed.shape,
            'rows_removed': df_original.shape[0] - df_transformed.shape[0],
            'columns_added': list(set(df_transformed.columns) - set(df_original.columns)),
            'data_types_after': df_transformed.dtypes.to_dict(),
            'memory_usage_mb_after': df_transformed.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        return metadata


# Example usage and testing
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create transformer instance
    transformer = DataTransformer()
    
    # Example: Transform sample data
    try:
        # Create sample data
        sample_data = {
            'Date': ['21-Aug-2020', '21-Aug-2020', '22-Aug-2020'],
            'Description': ['NEFT', 'Debit Card', 'ATM'],
            'Deposits': ['1,000.00', '00.00', '00.00'],
            'Withdrawls': ['00.00', '500.00', '200.00'],
            'Balance': ['10,000.00', '9,500.00', '9,300.00']
        }
        
        df = pd.DataFrame(sample_data)
        print("Original data:")
        print(df)
        
        # Transform data
        df_transformed = transformer.clean_bank_transactions(df)
        print("\nTransformed data:")
        print(df_transformed)
        
        # Create daily summary
        daily_summary = transformer.aggregate_daily_summary(df_transformed)
        print("\nDaily summary:")
        print(daily_summary)
        
    except Exception as e:
        print(f"Error: {str(e)}")
