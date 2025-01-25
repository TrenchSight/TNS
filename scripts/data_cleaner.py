# data_cleaner.py

import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """
    A class used to clean and normalize disparate data sources for the TrenchSight platform.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataCleaner with specific configurations.

        Parameters:
        - config (dict): Configuration parameters for data cleaning.
        """
        self.config = config
        logger.info("DataCleaner initialized with config: %s", config)

    def load_data(self, file_paths: List[str]) -> List[pd.DataFrame]:
        """
        Loads data from multiple file paths into pandas DataFrames.

        Parameters:
        - file_paths (list): List of file paths to load data from.

        Returns:
        - list of pd.DataFrame: Loaded data frames.
        """
        data_frames = []
        for path in file_paths:
            try:
                df = pd.read_csv(path)
                logger.info("Loaded data from %s with shape %s", path, df.shape)
                data_frames.append(df)
            except Exception as e:
                logger.error("Failed to load data from %s: %s", path, e)
        return data_frames

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans a single DataFrame by handling missing values, duplicates, and incorrect data types.

        Parameters:
        - df (pd.DataFrame): The DataFrame to clean.

        Returns:
        - pd.DataFrame: Cleaned DataFrame.
        """
        logger.info("Cleaning DataFrame with shape %s", df.shape)

        # Remove duplicates
        initial_shape = df.shape
        df = df.drop_duplicates()
        logger.debug("Dropped duplicates: %s -> %s", initial_shape, df.shape)

        # Handle missing values
        df = self.handle_missing_values(df)

        # Correct data types
        df = self.correct_data_types(df)

        # Normalize column names
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        logger.debug("Normalized column names: %s", df.columns.tolist())

        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing values in the DataFrame based on configuration.

        Parameters:
        - df (pd.DataFrame): The DataFrame to process.

        Returns:
        - pd.DataFrame: DataFrame with missing values handled.
        """
        strategy = self.config.get('missing_values', 'drop')
        logger.debug("Handling missing values with strategy: %s", strategy)

        if strategy == 'drop':
            df = df.dropna()
            logger.info("Dropped rows with missing values. New shape: %s", df.shape)
        elif strategy == 'fill':
            fill_values = self.config.get('fill_values', {})
            df = df.fillna(fill_values)
            logger.info("Filled missing values with: %s", fill_values)
        else:
            logger.warning("Unknown missing value strategy: %s. No action taken.", strategy)
        return df

    def correct_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Corrects data types of DataFrame columns based on configuration.

        Parameters:
        - df (pd.DataFrame): The DataFrame to process.

        Returns:
        - pd.DataFrame: DataFrame with corrected data types.
        """
        type_mappings = self.config.get('data_types', {})
        for column, dtype in type_mappings.items():
            if column in df.columns:
                try:
                    df[column] = df[column].astype(dtype)
                    logger.debug("Converted column %s to %s", column, dtype)
                except Exception as e:
                    logger.error("Failed to convert column %s to %s: %s", column, dtype, e)
        return df

    def normalize_data(self, data_frames: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Normalizes and merges multiple DataFrames into a single standardized DataFrame.

        Parameters:
        - data_frames (list of pd.DataFrame): List of DataFrames to normalize.

        Returns:
        - pd.DataFrame: Merged and normalized DataFrame.
        """
        logger.info("Normalizing and merging %d DataFrames", len(data_frames))
        cleaned_dfs = [self.clean_dataframe(df) for df in data_frames]
        merged_df = pd.concat(cleaned_dfs, ignore_index=True)
        logger.info("Merged DataFrame shape: %s", merged_df.shape)

        # Further normalization steps can be added here
        return merged_df

    def preprocess(self, file_paths: List[str]) -> pd.DataFrame:
        """
        Complete preprocessing pipeline: load, clean, and normalize data.

        Parameters:
        - file_paths (list): List of file paths to load data from.

        Returns:
        - pd.DataFrame: Fully preprocessed and normalized DataFrame.
        """
        logger.info("Starting preprocessing pipeline")
        data_frames = self.load_data(file_paths)
        normalized_data = self.normalize_data(data_frames)
        logger.info("Preprocessing pipeline completed with final shape: %s", normalized_data.shape)
        return normalized_data

if __name__ == "__main__":
    # Example configuration
    config = {
        'missing_values': 'fill',
        'fill_values': {
            'price': 0.0,
            'volume': 0.0,
            'sentiment': 'neutral'
        },
        'data_types': {
            'price': 'float',
            'volume': 'float',
            'timestamp': 'datetime64[ns]'
        }
    }

    # Initialize DataCleaner
    cleaner = DataCleaner(config)

    # Example file paths
    file_paths = [
        'data/blockchain_transactions.csv',
        'data/social_media_sentiment.csv',
        'data/market_news.csv'
    ]

    # Run preprocessing
    cleaned_data = cleaner.preprocess(file_paths)

    # Save the cleaned data
    cleaned_data.to_csv('data/cleaned_data.csv', index=False)
    logger.info("Cleaned data saved to data/cleaned_data.csv")

