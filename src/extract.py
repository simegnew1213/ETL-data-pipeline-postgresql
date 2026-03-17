"""
Data Extraction Module for Bank Transactions ETL Pipeline
Handles extraction of data from various sources (CSV, API, etc.)
"""

import pandas as pd
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import requests
from pathlib import Path


class DataExtractor:
    """Class responsible for extracting data from various sources."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the DataExtractor."""
        self.logger = logger or self._setup_logger()
        
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
    
    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional arguments for pd.read_csv()
            
        Returns:
            DataFrame containing the extracted data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            Exception: For other extraction errors
        """
        try:
            self.logger.info(f"Starting extraction from CSV: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            
            # Default parameters for CSV reading
            default_params = {
                'encoding': 'utf-8',
                'low_memory': False
            }
            default_params.update(kwargs)
            
            # Read CSV file
            df = pd.read_csv(file_path, **default_params)
            
            self.logger.info(f"Successfully extracted {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from CSV {file_path}: {str(e)}")
            raise
    
    def extract_from_url(self, url: str, params: Optional[Dict] = None, 
                        headers: Optional[Dict] = None) -> pd.DataFrame:
        """
        Extract data from URL (API or direct CSV download).
        
        Args:
            url: URL to extract data from
            params: Query parameters for the request
            headers: HTTP headers for the request
            
        Returns:
            DataFrame containing the extracted data
            
        Raises:
            Exception: For extraction errors
        """
        try:
            self.logger.info(f"Starting extraction from URL: {url}")
            
            # Make HTTP request
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            # Check if response is CSV
            content_type = response.headers.get('content-type', '')
            if 'text/csv' in content_type or url.endswith('.csv'):
                # Read CSV from string
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
            else:
                # Try to parse JSON
                df = pd.DataFrame(response.json())
            
            self.logger.info(f"Successfully extracted {len(df)} records from URL")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from URL {url}: {str(e)}")
            raise
    
    def extract_batch_csv(self, directory_path: str, pattern: str = "*.csv") -> pd.DataFrame:
        """
        Extract and combine data from multiple CSV files in a directory.
        
        Args:
            directory_path: Path to directory containing CSV files
            pattern: File pattern to match (default: "*.csv")
            
        Returns:
            Combined DataFrame from all matching files
            
        Raises:
            Exception: For extraction errors
        """
        try:
            self.logger.info(f"Starting batch extraction from directory: {directory_path}")
            
            directory = Path(directory_path)
            if not directory.exists():
                raise FileNotFoundError(f"Directory not found: {directory_path}")
            
            # Find all matching files
            csv_files = list(directory.glob(pattern))
            if not csv_files:
                raise ValueError(f"No CSV files found matching pattern '{pattern}'")
            
            # Read and combine all files
            dataframes = []
            for file_path in csv_files:
                self.logger.info(f"Processing file: {file_path}")
                df = self.extract_from_csv(str(file_path))
                df['source_file'] = file_path.name  # Add source file tracking
                dataframes.append(df)
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            self.logger.info(f"Successfully extracted {len(combined_df)} records from {len(csv_files)} files")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error in batch extraction: {str(e)}")
            raise
    
    def validate_extraction(self, df: pd.DataFrame, 
                          required_columns: Optional[list] = None,
                          min_rows: int = 1) -> bool:
        """
        Validate the extracted data.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            min_rows: Minimum number of rows expected
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check if DataFrame is empty
            if df.empty:
                self.logger.error("Extracted DataFrame is empty")
                return False
            
            # Check minimum row count
            if len(df) < min_rows:
                self.logger.error(f"Extracted data has {len(df)} rows, expected at least {min_rows}")
                return False
            
            # Check required columns
            if required_columns:
                missing_columns = set(required_columns) - set(df.columns)
                if missing_columns:
                    self.logger.error(f"Missing required columns: {missing_columns}")
                    return False
            
            # Check for null values in critical columns
            null_counts = df.isnull().sum()
            high_null_columns = null_counts[null_counts > len(df) * 0.5].index.tolist()
            if high_null_columns:
                self.logger.warning(f"Columns with >50% null values: {high_null_columns}")
            
            self.logger.info("Data extraction validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during extraction validation: {str(e)}")
            return False
    
    def get_extraction_metadata(self, df: pd.DataFrame, 
                              source: str) -> Dict[str, Any]:
        """
        Generate metadata about the extracted data.
        
        Args:
            df: Extracted DataFrame
            source: Data source description
            
        Returns:
            Dictionary containing metadata
        """
        metadata = {
            'source': source,
            'extraction_timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'data_types': df.dtypes.to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }
        
        return metadata


# Example usage and testing
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create extractor instance
    extractor = DataExtractor()
    
    # Example: Extract from the bank transactions CSV
    try:
        csv_path = "../data/raw/2000000 BT Records.csv"
        df = extractor.extract_from_csv(csv_path)
        
        # Validate extraction
        if extractor.validate_extraction(df, required_columns=['Date', 'Description', 'Deposits', 'Withdrawls', 'Balance']):
            print("Extraction successful!")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            
            # Get metadata
            metadata = extractor.get_extraction_metadata(df, csv_path)
            print(f"Metadata: {metadata}")
        else:
            print("Extraction validation failed!")
            
    except Exception as e:
        print(f"Error: {str(e)}")
