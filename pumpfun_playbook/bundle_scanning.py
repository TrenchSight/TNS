# bundle_scanning.py

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import asyncio
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("bundle_scanning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BundleScanningConfig:
    """
    Configuration for the Bundle Scanning module.
    """
    def __init__(self, config_path: str = 'config/bundle_scanning_config.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads bundle scanning configuration from a JSON file.
        """
        if not os.path.exists(self.config_path):
            logger.error("Bundle scanning configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")

        with open(self.config_path, 'r') as file:
            config = json.load(file)
        logger.info("Loaded bundle scanning configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_keys = ['kafka', 'ai_service', 'aggregation_rules']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("Bundle scanning configuration validated successfully.")

class KafkaConsumerWrapper:
    """
    Wrapper for Kafka Consumer to consume messages from multiple topics.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topics = kafka_config.get('topics', ['twitter_stream', 'telegram_stream', 'web_scans', 'wallet_scans'])
        self.group_id = kafka_config.get('group_id', 'bundle_scanning_group')
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        logger.info("Initialized KafkaConsumer with topics: %s", self.topics)

    def consume_messages(self):
        """
        Generator that yields messages from Kafka topics.
        """
        logger.info("Starting to consume messages from Kafka topics.")
        for message in self.consumer:
            logger.debug("Consumed message from %s: %s", message.topic, message.value)
            yield message.topic, message.value

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('output_topic', 'ai_processing')
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logger.info("Initialized KafkaProducer with servers: %s and topic: %s",
                    self.bootstrap_servers, self.topic)

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

class DataAggregator:
    """
    Aggregates data from multiple sources based on aggregation rules.
    """
    def __init__(self, aggregation_rules: Dict[str, Any]):
        self.aggregation_rules = aggregation_rules
        logger.info("DataAggregator initialized with rules: %s", self.aggregation_rules)

    def aggregate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aggregates incoming data according to the defined rules.

        Parameters:
        - data (dict): The incoming data from various sources.

        Returns:
        - dict: The aggregated data.
        """
        aggregated_data = {}
        for key, rule in self.aggregation_rules.items():
            source = rule.get('source')
            field = rule.get('field')
            operation = rule.get('operation', 'copy')

            if source in data and field in data[source]:
                if operation == 'copy':
                    aggregated_data[key] = data[source][field]
                elif operation == 'combine':
                    if key in aggregated_data:
                        aggregated_data[key].append(data[source][field])
                    else:
                        aggregated_data[key] = [data[source][field]]
                # Additional operations can be implemented here
            else:
                aggregated_data[key] = None
        logger.debug("Aggregated data: %s", aggregated_data)
        return aggregated_data

class AIProcessor:
    """
    Processes aggregated data using AI services.
    """
    def __init__(self, ai_service_config: Dict[str, Any]):
        self.api_endpoint = ai_service_config.get('api_endpoint')
        self.api_key = ai_service_config.get('api_key')
        if not self.api_endpoint or not self.api_key:
            logger.error("AI service configuration is incomplete.")
            raise ValueError("AI service configuration must include 'api_endpoint' and 'api_key'.")
        logger.info("AIProcessor initialized with endpoint: %s", self.api_endpoint)

    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends data to the AI service and returns the processed result.

        Parameters:
        - data (dict): The aggregated data.

        Returns:
        - dict: The AI-processed result.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.api_key}"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_endpoint, headers=headers, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.debug("AI service response: %s", result)
                        return result
                    else:
                        logger.error("AI service returned status %s", response.status)
                        return {}
        except Exception as e:
            logger.error("Failed to process data with AI service: %s", e)
            return {}

class BundleScanner:
    """
    Orchestrates the bundle scanning process: consuming, aggregating, processing, and producing.
    """
    def __init__(self, config: BundleScanningConfig):
        self.config = config
        self.consumer = KafkaConsumerWrapper(self.config.config['kafka'])
        self.producer = KafkaProducerWrapper(self.config.config['kafka'])
        self.aggregator = DataAggregator(self.config.config.get('aggregation_rules', {}))
        self.ai_processor = AIProcessor(self.config.config.get('ai_service', {}))
        logger.info("BundleScanner initialized.")

    async def run(self):
        """
        Runs the bundle scanning process asynchronously.
        """
        logger.info("Starting bundle scanning process.")
        async for topic, message in self.consume_messages_async():
            aggregated = self.aggregator.aggregate({topic: message})
            ai_result = await self.ai_processor.process(aggregated)
            if ai_result:
                self.producer.send(ai_result)
                logger.info("Processed and sent AI result for message from %s", topic)
            else:
                logger.warning("AI processing returned no result for message from %s", topic)

    async def consume_messages_async(self):
        """
        Asynchronously consumes messages from Kafka and yields them.
        """
        loop = asyncio.get_event_loop()
        for topic, message in self.consumer.consume_messages():
            yield topic, message

def load_wallets(wallets_path: str) -> List[Dict[str, str]]:
    """
    Loads wallet addresses to scan from a JSON file.
    """
    if not os.path.exists(wallets_path):
        logger.error("Wallets file %s does not exist.", wallets_path)
        return []

    with open(wallets_path, 'r') as file:
        wallets = json.load(file)
    logger.info("Loaded %d wallets from %s", len(wallets), wallets_path)
    return wallets

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize BundleScanning configuration
    config = BundleScanningConfig()

    # Initialize BundleScanner
    scanner = BundleScanner(config)

    # Run the bundle scanning process
    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        logger.info("Bundle scanning interrupted by user.")
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)

if __name__ == "__main__":
    main()

