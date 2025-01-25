# transaction_manager.py

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import yaml
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("transaction_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TransactionManagerConfig:
    """
    Configuration loader for Transaction Manager.
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
        required_keys = ['transaction_manager', 'kafka', 'database']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("TransactionManager configuration validated successfully.")

class KafkaConsumerWrapper:
    """
    Wrapper for Kafka Consumer to consume messages from a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('consumer_topic', 'wallet_scans')
        self.group_id = kafka_config.get('group_id', 'transaction_manager_group')
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info("Initialized KafkaConsumer for topic: %s", self.topic)

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
        self.topic = kafka_config.get('producer_topic', 'processed_transactions')
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logger.info("Initialized KafkaProducer for topic: %s", self.topic)

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

class DatabaseManager:
    """
    Manages database connections and operations.
    """
    def __init__(self, db_config: Dict[str, Any]):
        self.db_url = db_config.get('url')
        if not self.db_url:
            logger.error("Database URL not provided in configuration.")
            raise ValueError("Database URL must be provided in configuration.")
        
        try:
            self.engine = create_engine(self.db_url, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            self.metadata = MetaData()
            self.transactions_table = Table(
                'transactions', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('hash', String(66), unique=True, nullable=False),
                Column('from_address', String(42), nullable=False),
                Column('to_address', String(42), nullable=True),
                Column('value', Float, nullable=False),
                Column('gas', Integer, nullable=False),
                Column('gas_price', Float, nullable=False),
                Column('nonce', Integer, nullable=False),
                Column('block_number', Integer, nullable=False),
                Column('timestamp', DateTime, nullable=False),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            self.metadata.create_all(self.engine)
            logger.info("Database connected and transactions table ensured.")
        except SQLAlchemyError as e:
            logger.error("Database connection failed: %s", e)
            raise

    def insert_transaction(self, transaction: Dict[str, Any]):
        """
        Inserts a transaction record into the database.
        """
        session = self.Session()
        try:
            ins = self.transactions_table.insert().values(
                hash=transaction['hash'],
                from_address=transaction['from'],
                to_address=transaction.get('to'),
                value=transaction['value'],
                gas=transaction['gas'],
                gas_price=transaction['gasPrice'],
                nonce=transaction['nonce'],
                block_number=transaction['blockNumber'],
                timestamp=datetime.fromisoformat(transaction['timestamp'])
            )
            session.execute(ins)
            session.commit()
            logger.debug("Inserted transaction %s into database.", transaction['hash'])
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Failed to insert transaction %s: %s", transaction['hash'], e)
        finally:
            session.close()

class TransactionProcessor:
    """
    Processes raw transaction data.
    """
    def __init__(self, data_cleaner_config: Dict[str, Any]):
        self.missing_values_strategy = data_cleaner_config.get('missing_values', 'drop')
        self.fill_values = data_cleaner_config.get('fill_values', {})
        self.data_types = data_cleaner_config.get('data_types', {})
        logger.info("TransactionProcessor initialized with missing_values_strategy: %s", self.missing_values_strategy)

    def clean_transaction(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Cleans and normalizes a single transaction.
        """
        try:
            # Handle missing values
            for key, value in self.fill_values.items():
                if key not in transaction or transaction[key] is None:
                    if self.missing_values_strategy == 'fill':
                        transaction[key] = value
                    elif self.missing_values_strategy == 'drop':
                        logger.debug("Dropping transaction %s due to missing value in key: %s", transaction.get('hash'), key)
                        return None

            # Convert data types
            for key, dtype in self.data_types.items():
                if key in transaction:
                    if dtype == 'float':
                        transaction[key] = float(transaction[key])
                    elif dtype == 'int':
                        transaction[key] = int(transaction[key])
                    elif dtype == 'datetime':
                        transaction[key] = datetime.fromisoformat(transaction[key])
            logger.debug("Cleaned transaction: %s", transaction)
            return transaction
        except (ValueError, TypeError) as e:
            logger.error("Data type conversion failed for transaction %s: %s", transaction.get('hash'), e)
            return None

class TransactionManager:
    """
    Orchestrates the transaction management process.
    """
    def __init__(self, config: TransactionManagerConfig):
        self.config = config
        self.kafka_consumer = KafkaConsumerWrapper(self.config.config['kafka'])
        self.kafka_producer = KafkaProducerWrapper(self.config.config['kafka'])
        self.db_manager = DatabaseManager(self.config.config['database'])
        self.processor = TransactionProcessor(self.config.config.get('transaction_manager', {}))
        logger.info("TransactionManager initialized.")

    def run(self):
        """
        Runs the transaction management process.
        """
        logger.info("Starting TransactionManager process.")
        for message in self.kafka_consumer.consume_messages():
            cleaned_tx = self.processor.clean_transaction(message)
            if cleaned_tx:
                self.db_manager.insert_transaction(cleaned_tx)
                self.kafka_producer.send(cleaned_tx)
                logger.info("Processed and forwarded transaction %s", cleaned_tx.get('hash'))
            else:
                logger.warning("Transaction %s was not processed due to cleaning issues.", message.get('hash'))
        logger.info("TransactionManager process completed.")

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize configuration
    config = TransactionManagerConfig()

    # Initialize and run TransactionManager
    manager = TransactionManager(config)
    manager.run()

if __name__ == "__main__":
    main()

