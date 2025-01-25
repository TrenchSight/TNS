# launch_strategies.py

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

import yaml
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sklearn.externals import joblib
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("launch_strategies.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LaunchStrategiesConfig:
    """
    Configuration loader for Launch Strategies module.
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
            logger.error("Configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")
        
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info("Loaded configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_sections = ['launch_strategies', 'kafka', 'ai_models']
        for section in required_sections:
            if section not in self.config:
                logger.error("Missing required config section: %s", section)
                raise KeyError(f"Missing required config section: {section}")
        
        strategies_config = self.config['launch_strategies']
        required_keys = ['strategy_types', 'default_strategy']
        for key in required_keys:
            if key not in strategies_config:
                logger.error("Missing required config parameter in launch_strategies: %s", key)
                raise KeyError(f"Missing required config parameter in launch_strategies: {key}")
        
        ai_models_config = self.config['ai_models']
        if 'token_launch_model_path' not in ai_models_config:
            logger.error("Missing 'token_launch_model_path' in ai_models configuration.")
            raise KeyError("Missing 'token_launch_model_path' in ai_models configuration.")
        
        logger.info("LaunchStrategies configuration validated successfully.")

class KafkaConsumerWrapper:
    """
    Wrapper for Kafka Consumer to consume messages from a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('consumer_topic', 'token_launches')
        self.group_id = kafka_config.get('group_id', 'launch_strategies_group')
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info("Initialized KafkaConsumer for topic: %s", self.topic)
        except KafkaError as e:
            logger.error("Failed to initialize KafkaConsumer: %s", e)
            raise

    def consume_messages(self):
        """
        Generator that yields messages from the Kafka topic.
        """
        logger.info("Starting to consume messages from Kafka topic: %s", self.topic)
        for message in self.consumer:
            logger.debug("Consumed message: %s", message.value)
            yield message.value

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('producer_topic', 'launch_strategies_output')
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            logger.info("Initialized KafkaProducer for topic: %s", self.topic)
        except KafkaError as e:
            logger.error("Failed to initialize KafkaProducer: %s", e)
            raise

    def send(self, message: Dict[str, Any]):
        """
        Sends a message to the Kafka topic.
        """
        try:
            self.producer.send(self.topic, message)
            self.producer.flush()
            logger.debug("Sent message to Kafka topic %s: %s", self.topic, message)
        except KafkaError as e:
            logger.error("Failed to send message to Kafka: %s", e)

class AILaunchStrategyModel:
    """
    AI Model for assessing token launches.
    """
    def __init__(self, model_path: str):
        if not os.path.exists(model_path):
            logger.error("AI model file %s does not exist.", model_path)
            raise FileNotFoundError(f"AI model file {model_path} not found.")
        try:
            self.model = joblib.load(model_path)
            logger.info("Loaded AI launch strategy model from %s", model_path)
        except Exception as e:
            logger.error("Failed to load AI model: %s", e)
            raise

    def predict(self, features: List[Any]) -> str:
        """
        Predicts the strategy type based on input features.
        
        Parameters:
        - features (list): Feature list extracted from token launch data.
        
        Returns:
        - str: Predicted strategy type.
        """
        try:
            prediction = self.model.predict([features])[0]
            logger.debug("AI model prediction: %s", prediction)
            return prediction
        except Exception as e:
            logger.error("AI model prediction failed: %s", e)
            return "default"

class LaunchStrategiesManager:
    """
    Manages launch strategies based on AI assessments.
    """
    def __init__(self, config: LaunchStrategiesConfig):
        self.config = config.config['launch_strategies']
        self.kafka_consumer = KafkaConsumerWrapper(self.config['kafka'])
        self.kafka_producer = KafkaProducerWrapper(self.config['kafka'])
        ai_models_config = config.config['ai_models']
        self.ai_model = AILaunchStrategyModel(ai_models_config['token_launch_model_path'])
        self.strategy_types = self.config['strategy_types']
        self.default_strategy = self.config['default_strategy']
        logger.info("LaunchStrategiesManager initialized with default strategy: %s", self.default_strategy)

    def extract_features(self, data: Dict[str, Any]) -> List[Any]:
        """
        Extracts features from the token launch data for AI model prediction.
        
        Parameters:
        - data (dict): Token launch data.
        
        Returns:
        - list: Feature list.
        """
        try:
            features = [
                data.get('project_name', ''),
                data.get('tokenomics', {}).get('total_supply', 0),
                data.get('tokenomics', {}).get('liquidity_pool', 0),
                data.get('team_reputation', 0),
                data.get('market_sentiment', 0),
                data.get('social_media_engagement', 0),
                data.get('contract_audit', False)
            ]
            logger.debug("Extracted features: %s", features)
            return features
        except Exception as e:
            logger.error("Failed to extract features: %s", e)
            return []

    def select_strategy(self, prediction: str) -> Dict[str, Any]:
        """
        Selects the strategy configuration based on the AI prediction.
        
        Parameters:
        - prediction (str): Predicted strategy type.
        
        Returns:
        - dict: Strategy configuration.
        """
        strategy = self.strategy_types.get(prediction, self.strategy_types.get(self.default_strategy, {}))
        logger.info("Selected strategy: %s", prediction if prediction in self.strategy_types else self.default_strategy)
        return strategy

    def process_launch(self, launch_data: Dict[str, Any]):
        """
        Processes a single token launch data entry.
        
        Parameters:
        - launch_data (dict): Token launch data.
        """
        try:
            features = self.extract_features(launch_data)
            prediction = self.ai_model.predict(features)
            strategy = self.select_strategy(prediction)
            output = {
                'launch_id': launch_data.get('launch_id'),
                'project_name': launch_data.get('project_name'),
                'predicted_strategy': prediction,
                'strategy_details': strategy,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.kafka_producer.send(output)
            logger.info("Processed launch_id %s with strategy %s", launch_data.get('launch_id'), prediction)
        except Exception as e:
            logger.error("Failed to process launch data: %s", e)

    def run(self):
        """
        Runs the LaunchStrategiesManager to continuously process token launches.
        """
        logger.info("LaunchStrategiesManager started running.")
        for message in self.kafka_consumer.consume_messages():
            self.process_launch(message)
        logger.info("LaunchStrategiesManager stopped.")

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize configuration
    config = LaunchStrategiesConfig()

    # Initialize and run LaunchStrategiesManager
    manager = LaunchStrategiesManager(config)
    manager.run()

if __name__ == "__main__":
    main()

