# preprocess_data.py

import os
import json
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("preprocess_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Loads and validates the configuration from a YAML file.
    """

    def __init__(self, config_path: str = 'config.yaml'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads configuration from a YAML file.
        """
        if not os.path.exists(self.config_path):
            logger.error(f"Configuration file {self.config_path} does not exist.")
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")

        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {self.config_path}.")
            return config
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse configuration file {self.config_path}: {e}")
            raise

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_sections = ['data_preprocessing', 'data_cleaner', 'ai_model_features', 'data_split']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        dp_config = self.config['data_preprocessing']
        required_keys = ['output_path']
        for key in required_keys:
            if key not in dp_config:
                logger.error(f"Missing required config parameter in data_preprocessing: {key}")
                raise KeyError(f"Missing required config parameter in data_preprocessing: {key}")

        logger.info("Configuration validation successful.")


class DataPreprocessor:
    """
    Handles preprocessing of cleaned data including feature engineering, encoding, and scaling.
    """

    def __init__(self, config: Dict[str, Any]):
        self.input_path = config['data_cleaner'].get('output_path', 'data/cleaned_data.csv')
        self.output_path = config['data_preprocessing'].get('output_path', 'data/preprocessed_data.csv')
        self.feature_columns = config['ai_model_features'].get('token_launch', [])
        self.impute_strategy = config['data_preprocessing'].get('impute_strategy', 'mean')
        self.encoder_strategy = config['data_preprocessing'].get('encoder_strategy', 'onehot')
        self.scaler_strategy = config['data_preprocessing'].get('scaler_strategy', 'standard')
        self.random_state = config['data_split'].get('random_state', 42)

        # Define feature types based on feature_columns
        self.categorical_features = []
        self.numerical_features = []
        self.datetime_features = []

        # Initialize transformers
        self.column_transformer = None
        self.pipeline = None

        logger.info(f"DataPreprocessor initialized with input_path: {self.input_path}, output_path: {self.output_path}")
        self.identify_feature_types()
        self.build_pipeline()

    def identify_feature_types(self):
        """
        Identifies categorical, numerical, and datetime features based on feature_columns.
        """
        # Example heuristic: columns containing 'name', 'type', 'status' are categorical
        # Columns containing 'count', 'score', 'price' are numerical
        # Columns containing 'date', 'timestamp' are datetime
        for feature in self.feature_columns:
            if any(sub in feature.lower() for sub in ['name', 'type', 'status', 'category', 'address']):
                self.categorical_features.append(feature)
            elif any(sub in feature.lower() for sub in ['count', 'score', 'price', 'amount', 'total', 'percentage']):
                self.numerical_features.append(feature)
            elif any(sub in feature.lower() for sub in ['date', 'timestamp', 'time']):
                self.datetime_features.append(feature)
            else:
                # Default to numerical if unsure
                self.numerical_features.append(feature)

        logger.info(f"Identified categorical features: {self.categorical_features}")
        logger.info(f"Identified numerical features: {self.numerical_features}")
        logger.info(f"Identified datetime features: {self.datetime_features}")

    def build_pipeline(self):
        """
        Builds the preprocessing pipeline for the data.
        """
        # Define transformers for numerical and categorical features
        numerical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy=self.impute_strategy)),
            ('scaler', StandardScaler() if self.scaler_strategy == 'standard' else None)
        ])

        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('encoder', OneHotEncoder(handle_unknown='ignore') if self.encoder_strategy == 'onehot' else None)
        ])

        # Combine transformers using ColumnTransformer
        self.column_transformer = ColumnTransformer(
            transformers=[
                ('num', numerical_transformer, self.numerical_features),
                ('cat', categorical_transformer, self.categorical_features)
            ],
            remainder='drop'  # Drop other columns not specified
        )

        # Build the complete pipeline
        self.pipeline = Pipeline(steps=[
            ('column_transformer', self.column_transformer)
        ])

        logger.info("Preprocessing pipeline constructed successfully.")

    def load_data(self) -> pd.DataFrame:
        """
        Loads the cleaned data from the input path.
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

    def feature_engineering(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Performs feature engineering on the data.
        """
        logger.info("Starting feature engineering.")
        try:
            # Example: Extract year from timestamp if present
            if 'timestamp' in data.columns:
                data['year'] = pd.to_datetime(data['timestamp']).dt.year
                self.numerical_features.append('year')
                logger.info("Extracted 'year' from 'timestamp'.")

            # Example: Create interaction features
            # data['liquidity_supply_ratio'] = data['liquidity_pool'] / data['total_supply']
            if 'liquidity_pool' in data.columns and 'total_supply' in data.columns:
                data['liquidity_supply_ratio'] = data['liquidity_pool'] / data['total_supply']
                self.numerical_features.append('liquidity_supply_ratio')
                logger.info("Created 'liquidity_supply_ratio' feature.")

            # Drop original timestamp after extracting features
            if 'timestamp' in data.columns:
                data.drop('timestamp', axis=1, inplace=True)
                logger.info("Dropped 'timestamp' column after feature extraction.")

            return data
        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            raise

    def encode_and_scale(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies encoding and scaling to the data using the preprocessing pipeline.
        """
        logger.info("Starting encoding and scaling of features.")
        try:
            transformed_data = self.pipeline.fit_transform(data[self.feature_columns])
            logger.info(f"Transformed data shape: {transformed_data.shape}")

            # Get feature names after transformation
            transformed_feature_names = []
            if self.categorical_features:
                encoded_features = self.pipeline.named_steps['column_transformer'].named_transformers_['cat'].named_steps['encoder'].get_feature_names_out(self.categorical_features)
                transformed_feature_names.extend(encoded_features)
            if self.numerical_features:
                transformed_feature_names.extend(self.numerical_features)

            preprocessed_df = pd.DataFrame(transformed_data, columns=transformed_feature_names)
            logger.info("Encoding and scaling completed successfully.")
            return preprocessed_df
        except Exception as e:
            logger.error(f"Encoding and scaling failed: {e}")
            raise

    def preprocess(self) -> pd.DataFrame:
        """
        Orchestrates the preprocessing steps.
        """
        data = self.load_data()
        data = self.feature_engineering(data)
        preprocessed_data = self.encode_and_scale(data)
        logger.info("Data preprocessing pipeline executed successfully.")
        return preprocessed_data

    def save_preprocessed_data(self, data: pd.DataFrame):
        """
        Saves the preprocessed data to the output path.
        """
        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            data.to_csv(self.output_path, index=False)
            logger.info(f"Preprocessed data saved to {self.output_path}.")
        except Exception as e:
            logger.error(f"Failed to save preprocessed data to {self.output_path}: {e}")
            raise

    def run(self):
        """
        Executes the full preprocessing pipeline and saves the output.
        """
        try:
            preprocessed_data = self.preprocess()
            self.save_preprocessed_data(preprocessed_data)
            logger.info("Data preprocessing run completed successfully.")
        except Exception as e:
            logger.error(f"Data preprocessing run failed: {e}")


def main():
    """
    Main function to execute the data preprocessing.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Initialize configuration
    try:
        config_loader = ConfigLoader()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Initialize and run DataPreprocessor
    try:
        preprocessor = DataPreprocessor(config_loader.config)
        preprocessor.run()
    except Exception as e:
        logger.error(f"Data preprocessing failed: {e}")


if __name__ == "__main__":
    main()
