"""
Main ETL Pipeline for Bank Transactions Data Processing
Orchestrates the complete Extract, Transform, and Load process
"""

import os
import sys
import logging
import argparse
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader


class ETLPipeline:
    """Main ETL Pipeline class that orchestrates the entire process."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the ETL Pipeline.
        
        Args:
            config_file: Path to configuration file
        """
        self.config = self._load_config(config_file)
        self.logger = self._setup_logger()
        
        # Initialize ETL components
        self.extractor = DataExtractor(self.logger)
        self.transformer = DataTransformer(self.logger)
        self.loader = DataLoader(self.logger)
        
        # Pipeline statistics
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'extraction_stats': {},
            'transformation_stats': {},
            'loading_stats': {},
            'errors': []
        }
    
    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file or use defaults."""
        default_config = {
            'data_source': {
                'type': 'csv',
                'path': 'data/raw/2000000 BT Records.csv',
                'batch_size': 100000
            },
            'database': {
                'connection_string': None,  # Will use environment variables
                'batch_size': 10000,
                'truncate_tables': False
            },
            'logging': {
                'level': 'INFO',
                'file': 'logs/etl_pipeline.log',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'processing': {
                'create_daily_summary': True,
                'validate_data': True,
                'save_intermediate': True
            }
        }
        
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                user_config = json.load(f)
            # Merge with defaults
            default_config.update(user_config)
        
        return default_config
    
    def _setup_logger(self) -> logging.Logger:
        """Set up comprehensive logging."""
        logger = logging.getLogger('ETL_Pipeline')
        logger.setLevel(getattr(logging, self.config['logging']['level']))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create logs directory if it doesn't exist
        log_file = self.config['logging']['file']
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(self.config['logging']['format'])
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(self.config['logging']['format'])
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Returns:
            Dictionary containing pipeline execution statistics
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("Starting ETL Pipeline for Bank Transactions")
            self.logger.info("=" * 80)
            
            self.pipeline_stats['start_time'] = datetime.now()
            
            # Step 1: Extraction
            self.logger.info("Step 1: Starting Data Extraction")
            raw_data = self._extract_data()
            
            # Step 2: Transformation
            self.logger.info("Step 2: Starting Data Transformation")
            transformed_data = self._transform_data(raw_data)
            
            # Step 3: Loading
            self.logger.info("Step 3: Starting Data Loading")
            self._load_data(transformed_data)
            
            # Step 4: Generate summary (if configured)
            if self.config['processing']['create_daily_summary']:
                self.logger.info("Step 4: Creating Daily Summary")
                self._create_and_load_summary(transformed_data)
            
            self.pipeline_stats['end_time'] = datetime.now()
            self.pipeline_stats['total_duration'] = (
                self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
            ).total_seconds()
            
            self._log_pipeline_summary()
            return self.pipeline_stats
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            self.pipeline_stats['errors'].append(str(e))
            raise
        finally:
            # Cleanup
            self.loader.close_connections()
    
    def _extract_data(self) -> Any:
        """Extract data from the configured source."""
        try:
            start_time = time.time()
            
            data_source = self.config['data_source']
            
            if data_source['type'] == 'csv':
                data_path = data_source['path']
                if not os.path.isabs(data_path):
                    data_path = os.path.join(os.getcwd(), data_path)
                
                raw_data = self.extractor.extract_from_csv(data_path)
                
            elif data_source['type'] == 'url':
                raw_data = self.extractor.extract_from_url(data_source['path'])
                
            elif data_source['type'] == 'batch':
                raw_data = self.extractor.extract_batch_csv(
                    data_source['path'], 
                    data_source.get('pattern', '*.csv')
                )
            else:
                raise ValueError(f"Unsupported data source type: {data_source['type']}")
            
            # Validate extraction
            if self.config['processing']['validate_data']:
                if not self.extractor.validate_extraction(raw_data):
                    raise ValueError("Data extraction validation failed")
            
            # Get extraction metadata
            extraction_time = time.time() - start_time
            self.pipeline_stats['extraction_stats'] = {
                'records_extracted': len(raw_data),
                'extraction_time': round(extraction_time, 2),
                'extraction_rate': round(len(raw_data) / extraction_time, 2) if extraction_time > 0 else 0
            }
            
            self.logger.info(f"Extraction completed: {self.pipeline_stats['extraction_stats']}")
            
            # Save intermediate data if configured
            if self.config['processing']['save_intermediate']:
                self._save_intermediate_data(raw_data, 'raw_data.csv')
            
            return raw_data
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _transform_data(self, raw_data: Any) -> Any:
        """Transform the extracted data."""
        try:
            start_time = time.time()
            
            # Transform data
            transformed_data = self.transformer.clean_bank_transactions(raw_data)
            
            # Get transformation metadata
            transformation_time = time.time() - start_time
            self.pipeline_stats['transformation_stats'] = {
                'records_input': len(raw_data),
                'records_output': len(transformed_data),
                'records_removed': len(raw_data) - len(transformed_data),
                'transformation_time': round(transformation_time, 2),
                'transformation_rate': round(len(transformed_data) / transformation_time, 2) if transformation_time > 0 else 0
            }
            
            self.logger.info(f"Transformation completed: {self.pipeline_stats['transformation_stats']}")
            
            # Save intermediate data if configured
            if self.config['processing']['save_intermediate']:
                self._save_intermediate_data(transformed_data, 'transformed_data.csv')
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _load_data(self, transformed_data: Any) -> None:
        """Load transformed data into the database."""
        try:
            # Test database connection
            if not self.loader.test_connection():
                raise RuntimeError("Database connection failed")
            
            # Execute schema
            schema_path = os.path.join(os.getcwd(), 'config', 'database_schema.sql')
            self.loader.execute_schema(schema_path)
            
            # Load transactions
            load_stats = self.loader.load_transactions(
                transformed_data,
                batch_size=self.config['database']['batch_size'],
                truncate_table=self.config['database']['truncate_tables']
            )
            
            self.pipeline_stats['loading_stats'] = load_stats
            
            self.logger.info(f"Data loading completed: {load_stats}")
            
        except Exception as e:
            self.logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def _create_and_load_summary(self, transformed_data: Any) -> None:
        """Create and load daily summary data."""
        try:
            # Create daily summary
            daily_summary = self.transformer.aggregate_daily_summary(transformed_data)
            
            # Load summary data
            summary_stats = self.loader.upsert_daily_summary(daily_summary)
            
            self.pipeline_stats['summary_stats'] = summary_stats
            
            self.logger.info(f"Daily summary loading completed: {summary_stats}")
            
            # Save intermediate data if configured
            if self.config['processing']['save_intermediate']:
                self._save_intermediate_data(daily_summary, 'daily_summary.csv')
            
        except Exception as e:
            self.logger.error(f"Daily summary creation/loading failed: {str(e)}")
            # Don't raise error for summary failure - main pipeline can continue
    
    def _save_intermediate_data(self, data: Any, filename: str) -> None:
        """Save intermediate data for debugging."""
        try:
            output_path = os.path.join('data', 'processed', filename)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if hasattr(data, 'to_csv'):
                data.to_csv(output_path, index=False)
            else:
                with open(output_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            
            self.logger.info(f"Intermediate data saved to: {output_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to save intermediate data: {str(e)}")
    
    def _log_pipeline_summary(self) -> None:
        """Log pipeline execution summary."""
        self.logger.info("=" * 80)
        self.logger.info("ETL Pipeline Execution Summary")
        self.logger.info("=" * 80)
        
        stats = self.pipeline_stats
        self.logger.info(f"Total Duration: {stats['total_duration']:.2f} seconds")
        self.logger.info(f"Start Time: {stats['start_time']}")
        self.logger.info(f"End Time: {stats['end_time']}")
        
        self.logger.info("\nExtraction Statistics:")
        for key, value in stats['extraction_stats'].items():
            self.logger.info(f"  {key}: {value}")
        
        self.logger.info("\nTransformation Statistics:")
        for key, value in stats['transformation_stats'].items():
            self.logger.info(f"  {key}: {value}")
        
        self.logger.info("\nLoading Statistics:")
        for key, value in stats['loading_stats'].items():
            self.logger.info(f"  {key}: {value}")
        
        if 'summary_stats' in stats:
            self.logger.info("\nSummary Statistics:")
            for key, value in stats['summary_stats'].items():
                self.logger.info(f"  {key}: {value}")
        
        if stats['errors']:
            self.logger.error("\nErrors Encountered:")
            for error in stats['errors']:
                self.logger.error(f"  - {error}")
        
        # Get final database statistics
        try:
            db_stats = self.loader.get_load_statistics()
            self.logger.info("\nFinal Database Statistics:")
            for key, value in db_stats.items():
                self.logger.info(f"  {key}: {value}")
        except Exception as e:
            self.logger.warning(f"Could not retrieve database statistics: {str(e)}")
        
        self.logger.info("=" * 80)
    
    def save_pipeline_report(self, output_file: str = None) -> None:
        """Save pipeline execution report to file."""
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'logs/pipeline_report_{timestamp}.json'
        
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(self.pipeline_stats, f, indent=2, default=str)
            
            self.logger.info(f"Pipeline report saved to: {output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save pipeline report: {str(e)}")


def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(description='ETL Pipeline for Bank Transactions')
    parser.add_argument('--config', '-c', type=str, help='Configuration file path')
    parser.add_argument('--report', '-r', action='store_true', 
                       help='Save pipeline execution report')
    parser.add_argument('--output', '-o', type=str, 
                       help='Output file for pipeline report')
    
    args = parser.parse_args()
    
    try:
        # Initialize and run pipeline
        pipeline = ETLPipeline(args.config)
        stats = pipeline.run_pipeline()
        
        # Save report if requested
        if args.report:
            pipeline.save_pipeline_report(args.output)
        
        # Exit with appropriate code
        if stats['errors']:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
