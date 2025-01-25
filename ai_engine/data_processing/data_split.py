# data_split.py

import os
import json
import logging
from typing import Dict, Any, Optional, Tuple

import pandas as pd
from sklearn.model_selection import train_test_split
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("data_split.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataSplitter:
    """
    A class to handle splitting of cleaned data into training, validation, and test sets.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataSplitter with configuration parameters.

        Parameters:
        - config (dict): Configuration parameters for data splitting.
        """
        self.input_path = config.get('data_split', {}).get('input_path', 'data/cleaned_data.csv')
        self.output_dir = config.get('data_split', {}).get('output_dir', 'data/splits')
        self.train_size = config.get('data_split', {}).get('train_size', 0.7)
        self.validation_size = config.get('data_split', {}).get('validation_size', 0.15)
        self.test_size = config.get('data_split', {}).get('test_size', 0.15)
        self.random_state = config.get('data_split', {}).get('random_state', 42)
        self.stratify = config.get('data_split', {}).get('stratify', None)
        logger.info(f"DataSplitter initialized with input_path: {self.input_path}, output_dir: {self.output_dir}, "
                    f"train_size: {self.train_size}, validation_size: {self.validation_size}, "
                    f"test_size: {self.test_size}, random_state: {self.random_state}, stratify: {self.stratify}")

        # Validate split sizes
        total = self.train_size + self.validation_size + self.test_size
        if not 0.99 <= total <= 1.01:
            logger.error("Train, validation, and test sizes must sum to 1.0.")
            raise ValueError("Train, validation, and test sizes must sum to 1.0.")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Output directory '{self.output_dir}' is ready.")

    def load_data(self) -> pd.DataFrame:
        """
        Loads the cleaned data from the input path.

        Returns:
        - pd.DataFrame: The loaded data.
        """
        if not os.path.exists(self.input_path):
            logger.error(f"Input data file {self.input_path} does not exist.")
            raise FileNotFoundError(f"Input data file {self.input_path} not found.")
        
        try:
            data = pd.read_csv(self.input_path)
            logger.info(f"Loaded data from {self.input_path} with shape {data.shape}.")
            return data
        except Exception as e:
            logger.error(f"Failed to load data from {self.input_path}: {e}")
            raise

    def split_data(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Splits the data into training, validation, and test sets.

        Parameters:
        - data (pd.DataFrame): The cleaned data.

        Returns:
        - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Train, validation, and test dataframes.
        """
        logger.info("Starting data split into train, validation, and test sets.")

        try:
            train_val_data, test_data = train_test_split(
                data,
                test_size=self.test_size,
                random_state=self.random_state,
                stratify=data[self.stratify] if self.stratify else None
            )
            relative_val_size = self.validation_size / (self.train_size + self.validation_size)
            train_data, validation_data = train_test_split(
                train_val_data,
                test_size=relative_val_size,
                random_state=self.random_state,
                stratify=train_val_data[self.stratify] if self.stratify else None
            )
            logger.info(f"Data split completed: Train={train_data.shape}, Validation={validation_data.shape}, Test={test_data.shape}")
            return train_data, validation_data, test_data
        except Exception as e:
            logger.error(f"Failed to split data: {e}")
            raise

    def save_split(self, train: pd.DataFrame, validation: pd.DataFrame, test: pd.DataFrame):
        """
        Saves the split dataframes to CSV files.

        Parameters:
        - train (pd.DataFrame): Training data.
        - validation (pd.DataFrame): Validation data.
        - test (pd.DataFrame): Test data.
        """
        try:
            train_path = os.path.join(self.output_dir, 'train.csv')
            validation_path = os.path.join(self.output_dir, 'validation.csv')
            test_path = os.path.join(self.output_dir, 'test.csv')

            train.to_csv(train_path, index=False)
            validation.to_csv(validation_path, index=False)
            test.to_csv(test_path, index=False)

            logger.info(f"Saved training data to {train_path}")
            logger.info(f"Saved validation data to {validation_path}")
            logger.info(f"Saved test data to {test_path}")
        except Exception as e:
            logger.error(f"Failed to save split data: {e}")
            raise

    def run(self):
        """
        Executes the data splitting process.
        """
        data = self.load_data()
        train, validation, test = self.split_data(data)
        self.save_split(train, validation, test)
        logger.info("Data splitting process completed successfully.")

def load_configuration(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    Loads the configuration from a YAML file.

    Parameters:
    - config_path (str): Path to the configuration file.

    Returns:
    - dict: Configuration dictionary.
    """
    if not os.path.exists(config_path):
        logger.error(f"Configuration file {config_path} does not exist.")
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}.")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse configuration file {config_path}: {e}")
        raise

def main():
    """
    Main function to execute the data splitting process.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load configuration
    config = load_configuration()

    # Initialize and run DataSplitter
    try:
        splitter = DataSplitter(config)
        splitter.run()
    except Exception as e:
        logger.error(f"Data splitting process failed: {e}")

if __name__ == "__main__":
    main()

