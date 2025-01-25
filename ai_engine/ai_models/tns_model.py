# tns_model.py

import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
import yaml
from dotenv import load_dotenv

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("tns_model.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TNSModelConfig:
    """
    Configuration loader for TNS Model.
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
        required_sections = ['tns_model', 'kafka', 'database', 'preprocess_data']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        tns_model_config = self.config['tns_model']
        required_keys = ['model_path', 'scaler_path', 'encoder_path', 'features', 'target']
        for key in required_keys:
            if key not in tns_model_config:
                logger.error(f"Missing required config parameter in tns_model: {key}")
                raise KeyError(f"Missing required config parameter in tns_model: {key}")

        logger.info("TNSModel configuration validated successfully.")


class DatabaseManager:
    """
    Manages database connections and operations for storing and retrieving data.
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

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetches aggregated data from the database for model training and inference.

        Returns:
            pd.DataFrame: DataFrame containing aggregated data.
        """
        try:
            query = """
                SELECT 
                    ba.network,
                    ba.transaction_hash,
                    ba.from_address,
                    ba.to_address,
                    ba.value,
                    ba.gas,
                    ba.gas_price,
                    ba.nonce,
                    ba.block_number,
                    ba.timestamp,
                    ba.patterns_matched,
                    pa.project_name,
                    pa.price,
                    pa.volume,
                    pa.launch_date,
                    pa.contract_address,
                    pa.liquidity_locked,
                    pa.audit_status
                FROM 
                    blockchain_analysis ba
                INNER JOIN 
                    pumpfun_analysis pa
                ON 
                    ba.transaction_hash = pa.contract_address
                WHERE 
                    ba.patterns_matched IS NOT NULL
            """
            data = pd.read_sql(query, self.engine)
            logger.info(f"Fetched data from database with shape {data.shape}.")
            return data
        except Exception as e:
            logger.error(f"Failed to fetch data from database: {e}")
            raise

    def insert_insights(self, insights: Dict[str, Any]):
        """
        Inserts generated insights into the database.

        Args:
            insights (Dict[str, Any]): The insights to insert.
        """
        try:
            session = self.Session()
            # Placeholder: Define ORM model or use raw SQL to insert insights
            # Example using raw SQL
            insert_query = """
                INSERT INTO user_insights (
                    insight_type,
                    description,
                    related_projects,
                    timestamp
                ) VALUES (:insight_type, :description, :related_projects, :timestamp)
            """
            session.execute(insert_query, {
                'insight_type': insights.get('insight_type'),
                'description': insights.get('description'),
                'related_projects': json.dumps(insights.get('related_projects', [])),
                'timestamp': datetime.utcnow()
            })
            session.commit()
            logger.info("Inserted insights into database.")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to insert insights into database: {e}")
        finally:
            session.close()


class KafkaManager:
    """
    Manages Kafka consumer and producer for data streaming.
    """

    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.consumer_topic = kafka_config.get('consumer_topic', 'aggregated_data')
        self.producer_topic = kafka_config.get('producer_topic', 'tns_insights')
        self.group_id = kafka_config.get('group_id', 'tns_model_group')

        try:
            self.consumer = KafkaConsumer(
                self.consumer_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"KafkaConsumer initialized for topic: {self.consumer_topic}")
        except KafkaError as e:
            logger.error(f"Failed to initialize KafkaConsumer: {e}")
            raise

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            logger.info(f"KafkaProducer initialized for topic: {self.producer_topic}")
        except KafkaError as e:
            logger.error(f"Failed to initialize KafkaProducer: {e}")
            raise

    def consume_messages(self) -> Any:
        """
        Generator that yields messages from the consumer topic.

        Yields:
            Any: The message content.
        """
        logger.info("Starting to consume messages from Kafka.")
        for message in self.consumer:
            logger.debug(f"Consumed message: {message.value}")
            yield message.value

    def produce_insight(self, insight: Dict[str, Any]):
        """
        Sends an insight message to the producer topic.

        Args:
            insight (Dict[str, Any]): The insight to send.
        """
        try:
            self.producer.send(self.producer_topic, insight)
            self.producer.flush()
            logger.debug(f"Produced insight to {self.producer_topic}: {insight}")
        except KafkaError as e:
            logger.error(f"Failed to produce insight to Kafka: {e}")


class TNSModel:
    """
    The main AI model class for TrenchSight.
    Handles data ingestion, preprocessing, model training, prediction, and insight generation.
    """

    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager, kafka_manager: KafkaManager):
        self.config = config['tns_model']
        self.db_manager = db_manager
        self.kafka_manager = kafka_manager

        # Load preprocessing tools
        self.scaler = joblib.load(self.config['scaler_path']) if os.path.exists(self.config['scaler_path']) else StandardScaler()
        self.encoder = joblib.load(self.config['encoder_path']) if os.path.exists(self.config['encoder_path']) else OneHotEncoder(handle_unknown='ignore')
        logger.info("Preprocessing tools loaded.")

        # Load or initialize the model
        if os.path.exists(self.config['model_path']):
            self.model = joblib.load(self.config['model_path'])
            logger.info(f"Model loaded from {self.config['model_path']}.")
        else:
            self.model = RandomForestClassifier(n_estimators=100, random_state=42)
            logger.info("Initialized new RandomForestClassifier model.")

        # Define feature columns and target
        self.features = self.config['features']
        self.target = self.config['target']

        # Initialize column transformer
        self.column_transformer = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), self.features['numerical']),
                ('cat', OneHotEncoder(handle_unknown='ignore'), self.features['categorical'])
            ]
        )
        logger.info("ColumnTransformer initialized for preprocessing.")

    def preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocesses the data for model input.

        Args:
            data (pd.DataFrame): Raw aggregated data.

        Returns:
            pd.DataFrame: Preprocessed feature data.
        """
        try:
            X = data[self.features['numerical'] + self.features['categorical']]
            X_processed = self.column_transformer.transform(X)
            logger.info("Data preprocessing completed.")
            return pd.DataFrame(X_processed)
        except Exception as e:
            logger.error(f"Data preprocessing failed: {e}")
            raise

    def train_model(self, training_data: pd.DataFrame, training_labels: pd.Series):
        """
        Trains the AI model with the provided data.

        Args:
            training_data (pd.DataFrame): Preprocessed training features.
            training_labels (pd.Series): Training labels.
        """
        try:
            self.model.fit(training_data, training_labels)
            joblib.dump(self.model, self.config['model_path'])
            logger.info(f"Model trained and saved to {self.config['model_path']}.")
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            raise

    def generate_insights(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Generates insights based on model predictions.

        Args:
            data (pd.DataFrame): Preprocessed data for prediction.

        Returns:
            List[Dict[str, Any]]: List of generated insights.
        """
        try:
            predictions = self.model.predict(data)
            probabilities = self.model.predict_proba(data)[:, 1]
            insights = []
            for idx, prediction in enumerate(predictions):
                if prediction == 1:
                    insight = {
                        'insight_type': 'High_Potential_Token',
                        'description': f"Token {self.features['categorical'][0]} has high scalability potential.",
                        'related_projects': [self.features['categorical'][0]],
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    insights.append(insight)
            logger.info(f"Generated {len(insights)} insights.")
            return insights
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return []

    def run(self):
        """
        Runs the TNS Model: consumes data, preprocesses, predicts, and produces insights.
        """
        logger.info("TNS Model started running.")
        try:
            for message in self.kafka_manager.consume_messages():
                df = pd.DataFrame([message])
                preprocessed = self.preprocess_data(df)
                insights = self.generate_insights(preprocessed)
                for insight in insights:
                    self.kafka_manager.produce_insight(insight)
                    self.db_manager.insert_insights(insight)
            logger.info("TNS Model run completed.")
        except Exception as e:
            logger.error(f"TNS Model encountered an error: {e}")


def load_configuration(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    Loads the configuration from a YAML file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        Dict[str, Any]: Configuration dictionary.
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
    Main function to execute the TNS Model.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load configuration
    config = load_configuration()

    # Initialize DatabaseManager
    db_manager = DatabaseManager(config['database'])

    # Initialize KafkaManager
    kafka_manager = KafkaManager(config['kafka'])

    # Initialize and run TNSModel
    tns_model = TNSModel(config, db_manager, kafka_manager)
    tns_model.run()


if __name__ == "__main__":
    main()
