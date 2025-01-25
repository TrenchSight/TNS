# learning_model.py

import os
import json
import logging
import argparse
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models, optimizers, callbacks
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
import joblib
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("learning_model.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LearningModelConfig:
    """
    Configuration loader for Learning Model.
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
        required_sections = ['learning_model', 'data_split', 'preprocess_data']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        lm_config = self.config['learning_model']
        required_keys = ['model_save_path', 'label_column', 'features']
        for key in required_keys:
            if key not in lm_config:
                logger.error(f"Missing required config parameter in learning_model: {key}")
                raise KeyError(f"Missing required config parameter in learning_model: {key}")

        logger.info("LearningModel configuration validated successfully.")

class LearningModel:
    """
    A class to handle training, evaluation, and inference of the machine learning model.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the LearningModel with configuration parameters.

        Parameters:
        - config (dict): Configuration parameters for the learning model.
        """
        self.model_save_path = config['learning_model']['model_save_path']
        self.label_column = config['learning_model']['label_column']
        self.feature_columns = config['learning_model']['features']
        self.scaler_path = config['learning_model'].get('scaler_path', 'models/scaler.joblib')
        self.label_encoder_path = config['learning_model'].get('label_encoder_path', 'models/label_encoder.joblib')
        self.batch_size = config['learning_model'].get('batch_size', 32)
        self.epochs = config['learning_model'].get('epochs', 50)
        self.learning_rate = config['learning_model'].get('learning_rate', 0.001)
        self.dropout_rate = config['learning_model'].get('dropout_rate', 0.5)
        self.model_architecture = config['learning_model'].get('architecture', {
            'input_dim': len(self.feature_columns),
            'layers': [64, 32],
            'activation': 'relu'
        })
        logger.info(f"LearningModel initialized with model_save_path: {self.model_save_path}")

        # Initialize scaler and label encoder
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()

    def load_data(self, data_path: str) -> pd.DataFrame:
        """
        Loads the preprocessed data from a CSV file.

        Parameters:
        - data_path (str): Path to the preprocessed data CSV.

        Returns:
        - pd.DataFrame: Loaded data.
        """
        if not os.path.exists(data_path):
            logger.error(f"Data file {data_path} does not exist.")
            raise FileNotFoundError(f"Data file {data_path} not found.")

        try:
            data = pd.read_csv(data_path)
            logger.info(f"Loaded data from {data_path} with shape {data.shape}.")
            return data
        except Exception as e:
            logger.error(f"Failed to load data from {data_path}: {e}")
            raise

    def preprocess_features(self, data: pd.DataFrame, fit: bool = False) -> np.ndarray:
        """
        Scales the feature columns.

        Parameters:
        - data (pd.DataFrame): The data to scale.
        - fit (bool): Whether to fit the scaler.

        Returns:
        - np.ndarray: Scaled features.
        """
        features = data[self.feature_columns].values
        if fit:
            features = self.scaler.fit_transform(features)
            # Save the scaler
            os.makedirs(os.path.dirname(self.scaler_path), exist_ok=True)
            joblib.dump(self.scaler, self.scaler_path)
            logger.info(f"Scaler fitted and saved to {self.scaler_path}.")
        else:
            features = self.scaler.transform(features)
            logger.info("Features scaled using existing scaler.")
        return features

    def preprocess_labels(self, data: pd.DataFrame, fit: bool = False) -> np.ndarray:
        """
        Encodes the label column.

        Parameters:
        - data (pd.DataFrame): The data containing labels.
        - fit (bool): Whether to fit the label encoder.

        Returns:
        - np.ndarray: Encoded labels.
        """
        labels = data[self.label_column].values
        if fit:
            labels = self.label_encoder.fit_transform(labels)
            # Save the label encoder
            os.makedirs(os.path.dirname(self.label_encoder_path), exist_ok=True)
            joblib.dump(self.label_encoder, self.label_encoder_path)
            logger.info(f"Label encoder fitted and saved to {self.label_encoder_path}.")
        else:
            labels = self.label_encoder.transform(labels)
            logger.info("Labels encoded using existing label encoder.")
        return labels

    def build_model(self, input_dim: int) -> tf.keras.Model:
        """
        Builds the neural network model.

        Parameters:
        - input_dim (int): Number of input features.

        Returns:
        - tf.keras.Model: Compiled Keras model.
        """
        model = models.Sequential()
        model.add(layers.InputLayer(input_shape=(input_dim,)))

        for units in self.model_architecture['layers']:
            model.add(layers.Dense(units, activation=self.model_architecture['activation']))
            model.add(layers.Dropout(self.dropout_rate))

        model.add(layers.Dense(1, activation='sigmoid'))

        optimizer = optimizers.Adam(learning_rate=self.learning_rate)
        model.compile(optimizer=optimizer,
                      loss='binary_crossentropy',
                      metrics=['accuracy'])

        logger.info("Neural network model built successfully.")
        return model

    def train(self, train_data: pd.DataFrame, val_data: pd.DataFrame) -> tf.keras.Model:
        """
        Trains the neural network model.

        Parameters:
        - train_data (pd.DataFrame): Training data.
        - val_data (pd.DataFrame): Validation data.

        Returns:
        - tf.keras.Model: Trained Keras model.
        """
        X_train = self.preprocess_features(train_data, fit=True)
        y_train = self.preprocess_labels(train_data, fit=True)

        X_val = self.preprocess_features(val_data, fit=False)
        y_val = self.preprocess_labels(val_data, fit=False)

        model = self.build_model(input_dim=X_train.shape[1])

        early_stop = callbacks.EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)

        history = model.fit(
            X_train, y_train,
            epochs=self.epochs,
            batch_size=self.batch_size,
            validation_data=(X_val, y_val),
            callbacks=[early_stop],
            verbose=1
        )

        logger.info("Model training completed.")
        return model

    def evaluate(self, model: tf.keras.Model, test_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Evaluates the model on test data.

        Parameters:
        - model (tf.keras.Model): The trained Keras model.
        - test_data (pd.DataFrame): Test data.

        Returns:
        - Dict[str, Any]: Evaluation metrics.
        """
        X_test = self.preprocess_features(test_data, fit=False)
        y_test = self.preprocess_labels(test_data, fit=False)

        loss, accuracy = model.evaluate(X_test, y_test, verbose=0)
        y_pred_prob = model.predict(X_test).flatten()
        y_pred = (y_pred_prob >= 0.5).astype(int)

        report = classification_report(y_test, y_pred, output_dict=True)
        cm = confusion_matrix(y_test, y_pred).tolist()

        evaluation_results = {
            'loss': loss,
            'accuracy': accuracy,
            'classification_report': report,
            'confusion_matrix': cm
        }

        logger.info(f"Model evaluation results: Loss={loss}, Accuracy={accuracy}")
        return evaluation_results

    def save_model(self, model: tf.keras.Model):
        """
        Saves the trained model to disk.

        Parameters:
        - model (tf.keras.Model): The trained Keras model.
        """
        try:
            os.makedirs(os.path.dirname(self.model_save_path), exist_ok=True)
            model.save(self.model_save_path)
            logger.info(f"Model saved to {self.model_save_path}.")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise

    def load_model(self) -> tf.keras.Model:
        """
        Loads a trained model from disk.

        Returns:
        - tf.keras.Model: The loaded Keras model.
        """
        if not os.path.exists(self.model_save_path):
            logger.error(f"Model file {self.model_save_path} does not exist.")
            raise FileNotFoundError(f"Model file {self.model_save_path} not found.")

        try:
            model = models.load_model(self.model_save_path)
            logger.info(f"Model loaded from {self.model_save_path}.")
            return model
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

    def predict(self, input_data: pd.DataFrame) -> np.ndarray:
        """
        Makes predictions on new input data.

        Parameters:
        - input_data (pd.DataFrame): New data for prediction.

        Returns:
        - np.ndarray: Predicted probabilities.
        """
        X = self.preprocess_features(input_data, fit=False)
        predictions = self.model.predict(X).flatten()
        logger.info("Made predictions on input data.")
        return predictions

def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
    - argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="TrenchSight Learning Model Trainer and Evaluator")
    parser.add_argument('action', choices=['train', 'evaluate', 'save', 'load', 'predict'], help="Action to perform")
    parser.add_argument('--data_path', type=str, default='data/preprocessed_data.csv', help="Path to preprocessed data CSV")
    parser.add_argument('--model_path', type=str, help="Path to save/load the model")
    parser.add_argument('--config_path', type=str, default='config.yaml', help="Path to the configuration YAML file")
    parser.add_argument('--input_data', type=str, help="Path to input data for prediction")
    return parser.parse_args()

def main():
    """
    Main function to execute training, evaluation, saving, loading, or prediction.
    """
    args = parse_arguments()

    # Load configuration
    config_loader = LearningModelConfig(config_path=args.config_path)
    config = config_loader.config

    # Initialize LearningModel
    model = LearningModel(config)

    if args.action == 'train':
        logger.info("Starting model training.")
        # Split data into train and validation
        data = model.load_data(args.data_path)
        train_df, val_df = train_test_split(
            data,
            test_size=config['data_split']['validation_size'],
            random_state=config['learning_model']['random_state'],
            stratify=data[config['learning_model']['label_column']] if config['learning_model'].get('stratify', None) else None
        )
        trained_model = model.train(train_df, val_df)
        model.save_model(trained_model)
        logger.info("Model training and saving completed.")

    elif args.action == 'evaluate':
        logger.info("Starting model evaluation.")
        trained_model = model.load_model()
        data = model.load_data(args.data_path)
        evaluation_results = model.evaluate(trained_model, data)
        print(json.dumps(evaluation_results, indent=4))
        logger.info("Model evaluation completed.")

    elif args.action == 'save':
        logger.info("Saving the trained model.")
        trained_model = model.load_model()
        model.save_model(trained_model)
        logger.info("Model saved successfully.")

    elif args.action == 'load':
        logger.info("Loading the trained model.")
        trained_model = model.load_model()
        logger.info("Model loaded successfully.")

    elif args.action == 'predict':
        if not args.input_data:
            logger.error("Input data path must be provided for prediction.")
            raise ValueError("Input data path must be provided for prediction.")
        logger.info("Starting prediction on new data.")
        trained_model = model.load_model()
        input_data = model.load_data(args.input_data)
        predictions = model.predict(input_data)
        # Attach predictions to input data
        input_data['prediction_probability'] = predictions
        input_data['prediction'] = (predictions >= 0.5).astype(int)
        # Decode labels if label encoder exists
        if os.path.exists(model.label_encoder_path):
            label_encoder = joblib.load(model.label_encoder_path)
            input_data['predicted_label'] = label_encoder.inverse_transform(input_data['prediction'])
        output_path = os.path.join(os.path.dirname(args.input_data), 'predictions.csv')
        input_data.to_csv(output_path, index=False)
        logger.info(f"Predictions saved to {output_path}.")
        print(f"Predictions saved to {output_path}.")

    else:
        logger.error(f"Unknown action: {args.action}")
        raise ValueError(f"Unknown action: {args.action}")

if __name__ == "__main__":
    main()

