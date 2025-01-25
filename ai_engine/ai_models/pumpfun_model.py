# pumpfun_model.py

import os
import json
import logging
import argparse
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_auc_score
import joblib
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("pumpfun_model.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PumpFunModelConfig:
    """
    Configuration loader for PumpFun Model.
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
        required_sections = ['pumpfun_model', 'database', 'kafka', 'scanning_patterns']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        pumpfun_model_config = self.config['pumpfun_model']
        required_keys = ['model_save_path', 'scaler_path', 'label_encoder_path', 'features', 'label']
        for key in required_keys:
            if key not in pumpfun_model_config:
                logger.error(f"Missing required config parameter in pumpfun_model: {key}")
                raise KeyError(f"Missing required config parameter in pumpfun_model: {key}")

        logger.info("PumpFunModel configuration validated successfully.")


class DatabaseManager:
    """
    Manages database connections and operations for fetching PumpFun data.
    """

    def __init__(self, db_config: Dict[str, Any]):
        self.db_url = db_config.get('url')
        if not self.db_url:
            logger.error("Database URL not provided in configuration.")
            raise ValueError("Database URL must be provided in configuration.")

        try:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            self.engine = create_engine(self.db_url, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("DatabaseManager initialized and connected to the database.")
        except Exception as e:
            logger.error(f"Failed to initialize DatabaseManager: {e}")
            raise

    def fetch_pumpfun_data(self) -> pd.DataFrame:
        """
        Fetches PumpFun analysis data from the database.

        Returns:
            pd.DataFrame: DataFrame containing PumpFun data.
        """
        try:
            query = """
                SELECT project_name, price, volume, launch_date, contract_address, liquidity_locked, audit_status
                FROM pumpfun_analysis
                WHERE patterns_matched IS NOT NULL
            """
            data = pd.read_sql(query, self.engine)
            logger.info(f"Fetched PumpFun data with shape {data.shape}.")
            return data
        except Exception as e:
            logger.error(f"Failed to fetch PumpFun data: {e}")
            raise


class PumpFunModel:
    """
    Handles training, evaluation, saving, loading, and prediction of the PumpFun ML model.
    """

    def __init__(self, config: Dict[str, Any]):
        self.model_save_path = config['pumpfun_model']['model_save_path']
        self.scaler_path = config['pumpfun_model']['scaler_path']
        self.label_encoder_path = config['pumpfun_model']['label_encoder_path']
        self.features = config['pumpfun_model']['features']
        self.label = config['pumpfun_model']['label']
        self.model = None
        self.scaler = None
        self.label_encoder = None
        logger.info(f"PumpFunModel initialized with model_save_path: {self.model_save_path}")

    def load_data(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepares features and labels from the data.

        Args:
            data (pd.DataFrame): Raw PumpFun data.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: Features and labels.
        """
        try:
            X = data[self.features]
            y = data[self.label]

            # Encode labels
            self.label_encoder = LabelEncoder()
            y_encoded = self.label_encoder.fit_transform(y)
            logger.info("Labels encoded successfully.")

            # Scale features
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X)
            logger.info("Features scaled successfully.")

            return pd.DataFrame(X_scaled, columns=self.features), pd.Series(y_encoded)
        except Exception as e:
            logger.error(f"Failed to preprocess data: {e}")
            raise

    def train(self, X_train: pd.DataFrame, y_train: pd.Series,
              X_val: pd.DataFrame, y_val: pd.Series) -> Any:
        """
        Trains the machine learning model with hyperparameter tuning.

        Args:
            X_train (pd.DataFrame): Training features.
            y_train (pd.Series): Training labels.
            X_val (pd.DataFrame): Validation features.
            y_val (pd.Series): Validation labels.

        Returns:
            Any: Trained model.
        """
        try:
            logger.info("Starting model training with hyperparameter tuning.")
            param_grid = {
                'n_estimators': [100, 200, 300],
                'max_depth': [None, 10, 20, 30],
                'min_samples_split': [2, 5, 10]
            }
            rf = RandomForestClassifier(random_state=42)
            grid_search = GridSearchCV(rf, param_grid, cv=5, scoring='accuracy', n_jobs=-1)
            grid_search.fit(X_train, y_train)
            best_model = grid_search.best_estimator_
            logger.info(f"Model training completed. Best parameters: {grid_search.best_params_}")
            return best_model
        except Exception as e:
            logger.error(f"Failed to train the model: {e}")
            raise

    def evaluate(self, model: Any, X_test: pd.DataFrame, y_test: pd.Series) -> Dict[str, Any]:
        """
        Evaluates the trained model on test data.

        Args:
            model (Any): Trained machine learning model.
            X_test (pd.DataFrame): Test features.
            y_test (pd.Series): Test labels.

        Returns:
            Dict[str, Any]: Evaluation metrics.
        """
        try:
            logger.info("Starting model evaluation.")
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1]

            accuracy = accuracy_score(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            roc_auc = roc_auc_score(y_test, y_proba)
            report = classification_report(y_test, y_pred, output_dict=True)
            cm = confusion_matrix(y_test, y_pred).tolist()

            evaluation_metrics = {
                'accuracy': accuracy,
                'mse': mse,
                'roc_auc': roc_auc,
                'classification_report': report,
                'confusion_matrix': cm
            }

            logger.info(f"Model Evaluation - Accuracy: {accuracy}, MSE: {mse}, ROC AUC: {roc_auc}")
            return evaluation_metrics
        except Exception as e:
            logger.error(f"Failed to evaluate the model: {e}")
            raise

    def save_model(self, model: Any):
        """
        Saves the trained model, scaler, and label encoder to disk.

        Args:
            model (Any): Trained machine learning model.
        """
        try:
            os.makedirs(os.path.dirname(self.model_save_path), exist_ok=True)
            joblib.dump(model, self.model_save_path)
            logger.info(f"Model saved to {self.model_save_path}.")

            joblib.dump(self.scaler, self.scaler_path)
            logger.info(f"Scaler saved to {self.scaler_path}.")

            joblib.dump(self.label_encoder, self.label_encoder_path)
            logger.info(f"Label encoder saved to {self.label_encoder_path}.")
        except Exception as e:
            logger.error(f"Failed to save model components: {e}")
            raise

    def load_model_components(self):
        """
        Loads the trained model, scaler, and label encoder from disk.
        """
        try:
            self.model = joblib.load(self.model_save_path)
            logger.info(f"Model loaded from {self.model_save_path}.")

            self.scaler = joblib.load(self.scaler_path)
            logger.info(f"Scaler loaded from {self.scaler_path}.")

            self.label_encoder = joblib.load(self.label_encoder_path)
            logger.info(f"Label encoder loaded from {self.label_encoder_path}.")
        except Exception as e:
            logger.error(f"Failed to load model components: {e}")
            raise

    def predict(self, data: pd.DataFrame) -> np.ndarray:
        """
        Makes predictions on new data.

        Args:
            data (pd.DataFrame): New data for prediction.

        Returns:
            np.ndarray: Predicted labels.
        """
        try:
            X = data[self.features]
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            logger.info("Predictions made successfully.")
            return predictions
        except Exception as e:
            logger.error(f"Failed to make predictions: {e}")
            raise


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="PumpFun Machine Learning Model Trainer and Evaluator")
    parser.add_argument('action', choices=['train', 'evaluate', 'save_model', 'load_model', 'predict'], help="Action to perform")
    parser.add_argument('--config_path', type=str, default='config.yaml', help="Path to the configuration YAML file")
    parser.add_argument('--data_path', type=str, help="Path to the data CSV file")
    parser.add_argument('--model_path', type=str, help="Path to save/load the model")
    parser.add_argument('--input_data', type=str, help="Path to input data for prediction")
    parser.add_argument('--output_predictions', type=str, help="Path to save prediction results")
    return parser.parse_args()


def main():
    """
    Main function to execute training, evaluation, saving, loading, or prediction.
    """
    args = parse_arguments()

    # Load configuration
    config_loader = PumpFunModelConfig(config_path=args.config_path)
    config = config_loader.config

    # Initialize PumpFunModel
    pumpfun_model = PumpFunModel(config)

    if args.action == 'train':
        logger.info("Starting model training.")
        if not args.data_path:
            logger.error("Data path must be provided for training.")
            raise ValueError("Data path must be provided for training.")

        # Fetch data from the database
        db_manager = DatabaseManager(config['database'])
        data = db_manager.fetch_pumpfun_data()

        # Preprocess data
        X, y = pumpfun_model.load_data(data)

        # Split data
        X_train, X_temp, y_train, y_temp = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )
        X_val, X_test, y_val, y_test = train_test_split(
            X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp
        )
        logger.info(f"Data split into train: {X_train.shape}, validation: {X_val.shape}, test: {X_test.shape}")

        # Train model
        trained_model = pumpfun_model.train(X_train, y_train, X_val, y_val)

        # Evaluate model
        evaluation_metrics = pumpfun_model.evaluate(trained_model, X_test, y_test)
        print(json.dumps(evaluation_metrics, indent=4))
        logger.info("Model evaluation completed.")

        # Save model
        pumpfun_model.save_model(trained_model)

    elif args.action == 'evaluate':
        logger.info("Starting model evaluation.")
        if not args.data_path:
            logger.error("Data path must be provided for evaluation.")
            raise ValueError("Data path must be provided for evaluation.")

        # Load model components
        pumpfun_model.load_model_components()

        # Fetch data from the database
        db_manager = DatabaseManager(config['database'])
        data = db_manager.fetch_pumpfun_data()

        # Preprocess data
        X, y = pumpfun_model.load_data(data)

        # Split data
        X_train, X_temp, y_train, y_temp = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )
        X_val, X_test, y_val, y_test = train_test_split(
            X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp
        )
        logger.info(f"Data split into train: {X_train.shape}, validation: {X_val.shape}, test: {X_test.shape}")

        # Evaluate model
        evaluation_metrics = pumpfun_model.evaluate(pumpfun_model.model, X_test, y_test)
        print(json.dumps(evaluation_metrics, indent=4))
        logger.info("Model evaluation completed.")

    elif args.action == 'save_model':
        logger.info("Saving the trained model.")
        pumpfun_model.save_model(pumpfun_model.model)
        logger.info("Model saved successfully.")

    elif args.action == 'load_model':
        logger.info("Loading the trained model.")
        pumpfun_model.load_model_components()
        logger.info("Model loaded successfully.")

    elif args.action == 'predict':
        logger.info("Starting prediction on new data.")
        if not args.input_data:
            logger.error("Input data path must be provided for prediction.")
            raise ValueError("Input data path must be provided for prediction.")

        if not args.output_predictions:
            logger.error("Output predictions path must be provided.")
            raise ValueError("Output predictions path must be provided.")

        # Load model components
        pumpfun_model.load_model_components()

        # Load input data
        input_data = pd.read_csv(args.input_data)
        logger.info(f"Loaded input data from {args.input_data} with shape {input_data.shape}.")

        # Preprocess input data
        try:
            X_new = input_data[pumpfun_model.features]
            X_scaled = pumpfun_model.scaler.transform(X_new)
            logger.info("Input data scaled successfully.")
        except Exception as e:
            logger.error(f"Failed to preprocess input data: {e}")
            raise

        # Make predictions
        predictions = pumpfun_model.model.predict(X_scaled)
        input_data['prediction_probability'] = predictions
        input_data['prediction'] = (predictions >= 0.5).astype(int)
        input_data['predicted_label'] = pumpfun_model.label_encoder.inverse_transform(input_data['prediction'])

        # Save predictions
        os.makedirs(os.path.dirname(args.output_predictions), exist_ok=True)
        input_data.to_csv(args.output_predictions, index=False)
        logger.info(f"Predictions saved to {args.output_predictions}.")
        print(f"Predictions saved to {args.output_predictions}.")

    else:
        logger.error(f"Unknown action: {args.action}")
        raise ValueError(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()

