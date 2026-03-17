"""
Utility functions for the ETL Pipeline
Contains helper functions for error handling, logging, and monitoring
"""

import logging
import traceback
import functools
import time
import psutil
import os
from typing import Any, Callable, Dict, Optional
from datetime import datetime
import json


class ETLPipelineError(Exception):
    """Custom exception for ETL pipeline errors."""
    pass


class PipelineMonitor:
    """Monitor pipeline performance and system resources."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = None
        self.checkpoints = []
    
    def start_monitoring(self):
        """Start monitoring pipeline execution."""
        self.start_time = time.time()
        self.logger.info("Pipeline monitoring started")
        self._log_system_status()
    
    def add_checkpoint(self, name: str, metadata: Optional[Dict] = None):
        """Add a checkpoint with timing and system status."""
        if self.start_time is None:
            self.start_time = time.time()
        
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        checkpoint = {
            'name': name,
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': round(elapsed, 2),
            'metadata': metadata or {}
        }
        
        # Add system metrics
        checkpoint['system_metrics'] = self._get_system_metrics()
        
        self.checkpoints.append(checkpoint)
        self.logger.info(f"Checkpoint '{name}' reached at {elapsed:.2f}s")
    
    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics."""
        try:
            process = psutil.Process()
            return {
                'cpu_percent': psutil.cpu_percent(),
                'memory_percent': psutil.virtual_memory().percent,
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'disk_usage_percent': psutil.disk_usage('/').percent
            }
        except Exception as e:
            self.logger.warning(f"Could not get system metrics: {str(e)}")
            return {}
    
    def _log_system_status(self):
        """Log current system status."""
        metrics = self._get_system_metrics()
        self.logger.info(f"System Status: {metrics}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get monitoring summary."""
        if not self.checkpoints:
            return {}
        
        total_time = self.checkpoints[-1]['elapsed_seconds']
        
        return {
            'total_duration_seconds': total_time,
            'checkpoints': self.checkpoints,
            'checkpoint_count': len(self.checkpoints),
            'average_checkpoint_time': total_time / len(self.checkpoints) if self.checkpoints else 0
        }


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, 
                    exceptions: tuple = (Exception,)):
    """
    Decorator to retry function execution on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
        exceptions: Tuple of exceptions to catch and retry on
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = kwargs.get('logger') or logging.getLogger(func.__module__)
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                        raise
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
            
        return wrapper
    return decorator


def log_execution_time(func: Callable) -> Callable:
    """Decorator to log function execution time."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = kwargs.get('logger') or logging.getLogger(func.__module__)
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} completed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds: {str(e)}")
            raise
    
    return wrapper


def validate_file_path(file_path: str, must_exist: bool = True, 
                      file_type: str = 'file') -> bool:
    """
    Validate file path and existence.
    
    Args:
        file_path: Path to validate
        must_exist: Whether the file must exist
        file_type: Type of path ('file' or 'directory')
    
    Returns:
        True if validation passes
    
    Raises:
        ETLPipelineError: If validation fails
    """
    if not file_path:
        raise ETLPipelineError("File path cannot be empty")
    
    if must_exist:
        if file_type == 'file' and not os.path.isfile(file_path):
            raise ETLPipelineError(f"File does not exist: {file_path}")
        elif file_type == 'directory' and not os.path.isdir(file_path):
            raise ETLPipelineError(f"Directory does not exist: {file_path}")
    
    # Check if parent directory exists for new files
    if not must_exist:
        parent_dir = os.path.dirname(file_path)
        if parent_dir and not os.path.exists(parent_dir):
            raise ETLPipelineError(f"Parent directory does not exist: {parent_dir}")
    
    return True


def safe_file_operation(operation: str):
    """
    Decorator for safe file operations with error handling.
    
    Args:
        operation: Description of the file operation
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = kwargs.get('logger') or logging.getLogger(func.__module__)
            
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                logger.error(f"File not found during {operation}: {str(e)}")
                raise ETLPipelineError(f"File operation failed: {operation}")
            except PermissionError as e:
                logger.error(f"Permission denied during {operation}: {str(e)}")
                raise ETLPipelineError(f"Permission denied: {operation}")
            except OSError as e:
                logger.error(f"OS error during {operation}: {str(e)}")
                raise ETLPipelineError(f"File system error: {operation}")
            except Exception as e:
                logger.error(f"Unexpected error during {operation}: {str(e)}")
                raise ETLPipelineError(f"Unexpected error: {operation}")
        
        return wrapper
    return decorator


def create_error_report(error: Exception, context: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create a detailed error report.
    
    Args:
        error: Exception that occurred
        context: Additional context information
    
    Returns:
        Dictionary containing error report
    """
    error_report = {
        'timestamp': datetime.now().isoformat(),
        'error_type': type(error).__name__,
        'error_message': str(error),
        'traceback': traceback.format_exc(),
        'context': context or {}
    }
    
    return error_report


def save_error_report(error_report: Dict[str, Any], 
                     output_dir: str = 'logs') -> str:
    """
    Save error report to file.
    
    Args:
        error_report: Error report dictionary
        output_dir: Directory to save the report
    
    Returns:
        Path to the saved error report file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"error_report_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    try:
        with open(filepath, 'w') as f:
            json.dump(error_report, f, indent=2, default=str)
        
        return filepath
    except Exception as e:
        logging.error(f"Failed to save error report: {str(e)}")
        return ""


class DataValidator:
    """Utility class for data validation."""
    
    @staticmethod
    def validate_dataframe(df, required_columns: list = None, 
                          min_rows: int = 1, max_null_ratio: float = 0.5) -> Dict[str, Any]:
        """
        Validate DataFrame structure and content.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            min_rows: Minimum number of rows expected
            max_null_ratio: Maximum allowed ratio of null values per column
        
        Returns:
            Dictionary containing validation results
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }
        
        try:
            # Check if DataFrame is empty
            if df.empty:
                validation_results['is_valid'] = False
                validation_results['errors'].append("DataFrame is empty")
                return validation_results
            
            # Basic statistics
            validation_results['statistics'] = {
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            }
            
            # Check minimum rows
            if len(df) < min_rows:
                validation_results['is_valid'] = False
                validation_results['errors'].append(f"DataFrame has {len(df)} rows, expected at least {min_rows}")
            
            # Check required columns
            if required_columns:
                missing_columns = set(required_columns) - set(df.columns)
                if missing_columns:
                    validation_results['is_valid'] = False
                    validation_results['errors'].append(f"Missing required columns: {missing_columns}")
            
            # Check null ratios
            null_ratios = df.isnull().sum() / len(df)
            high_null_columns = null_ratios[null_ratios > max_null_ratio].index.tolist()
            if high_null_columns:
                validation_results['warnings'].append(f"Columns with >{max_null_ratio*100}% null values: {high_null_columns}")
            
            # Check for duplicates
            duplicate_count = df.duplicated().sum()
            if duplicate_count > 0:
                validation_results['warnings'].append(f"Found {duplicate_count} duplicate rows")
            
        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation failed: {str(e)}")
        
        return validation_results
    
    @staticmethod
    def validate_data_types(df, expected_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate DataFrame column data types.
        
        Args:
            df: DataFrame to validate
            expected_types: Dictionary mapping column names to expected pandas dtypes
        
        Returns:
            Dictionary containing type validation results
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        for column, expected_type in expected_types.items():
            if column not in df.columns:
                validation_results['errors'].append(f"Column '{column}' not found")
                validation_results['is_valid'] = False
                continue
            
            actual_type = str(df[column].dtype)
            
            # Allow for some type flexibility (e.g., int64 vs int32)
            if not actual_type.startswith(expected_type):
                validation_results['warnings'].append(
                    f"Column '{column}' has type '{actual_type}', expected '{expected_type}'"
                )
        
        return validation_results


# Example usage
if __name__ == "__main__":
    # Test utility functions
    logging.basicConfig(level=logging.INFO)
    
    # Test monitor
    monitor = PipelineMonitor()
    monitor.start_monitoring()
    
    time.sleep(1)
    monitor.add_checkpoint("Test checkpoint 1")
    
    time.sleep(0.5)
    monitor.add_checkpoint("Test checkpoint 2", {"test": "metadata"})
    
    summary = monitor.get_summary()
    print(f"Monitor summary: {json.dumps(summary, indent=2, default=str)}")
