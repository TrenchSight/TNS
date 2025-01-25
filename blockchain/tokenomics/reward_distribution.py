# reward_distribution.py

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import yaml
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import ContractLogicError
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
        logging.FileHandler("reward_distribution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RewardDistributionConfig:
    """
    Configuration loader for Reward Distribution module.
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
        required_keys = ['reward_distribution', 'kafka', 'database', 'blockchain']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("RewardDistribution configuration validated successfully.")

class KafkaConsumerWrapper:
    """
    Wrapper for Kafka Consumer to consume messages from a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('consumer_topic', 'ai_processing')
        self.group_id = kafka_config.get('group_id', 'reward_distribution_group')
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
        self.topic = kafka_config.get('producer_topic', 'reward_distribution')
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
            self.rewards_table = Table(
                'rewards', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('user_address', String(42), nullable=False),
                Column('amount', Float, nullable=False),
                Column('tx_hash', String(66), unique=True, nullable=False),
                Column('status', String(20), nullable=False, default='pending'),
                Column('created_at', DateTime, default=datetime.utcnow),
                Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
            )
            self.metadata.create_all(self.engine)
            logger.info("Database connected and rewards table ensured.")
        except SQLAlchemyError as e:
            logger.error("Database connection failed: %s", e)
            raise

    def insert_reward(self, reward: Dict[str, Any]):
        """
        Inserts a reward record into the database.
        """
        session = self.Session()
        try:
            ins = self.rewards_table.insert().values(
                user_address=reward['user_address'],
                amount=reward['amount'],
                tx_hash=reward['tx_hash'],
                status=reward.get('status', 'pending'),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            session.execute(ins)
            session.commit()
            logger.debug("Inserted reward %s into database.", reward['tx_hash'])
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Failed to insert reward %s: %s", reward['tx_hash'], e)
        finally:
            session.close()

    def update_reward_status(self, tx_hash: str, status: str):
        """
        Updates the status of a reward in the database.
        """
        session = self.Session()
        try:
            upd = self.rewards_table.update().where(
                self.rewards_table.c.tx_hash == tx_hash
            ).values(
                status=status,
                updated_at=datetime.utcnow()
            )
            session.execute(upd)
            session.commit()
            logger.debug("Updated reward %s to status %s.", tx_hash, status)
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Failed to update reward %s: %s", tx_hash, e)
        finally:
            session.close()

class SmartContractManager:
    """
    Manages interactions with the reward distribution smart contract.
    """
    def __init__(self, blockchain_config: Dict[str, Any], db_manager: DatabaseManager):
        self.blockchain_config = blockchain_config
        self.db_manager = db_manager
        self.web3_connections = self.initialize_web3_connections()
        self.contracts = self.load_contracts()
        logger.info("SmartContractManager initialized with contracts: %s", list(self.contracts.keys()))

    def initialize_web3_connections(self) -> Dict[str, Web3]:
        """
        Initializes Web3 connections for each configured network.
        """
        connections = {}
        for network, params in self.blockchain_config['networks'].items():
            rpc_url = params.get('rpc_url')
            if rpc_url:
                web3 = Web3(Web3.HTTPProvider(rpc_url))
                if web3.isConnected():
                    connections[network] = web3
                    logger.info("Connected to network %s", network)
                else:
                    logger.error("Failed to connect to network %s at %s", network, rpc_url)
            else:
                logger.error("RPC URL not provided for network %s", network)
        return connections

    def load_contracts(self) -> Dict[str, Any]:
        """
        Loads smart contract instances based on deployment information.
        """
        contracts = {}
        contracts_path = self.blockchain_config.get('contracts_path', 'build')
        for contract_info in self.blockchain_config.get('contracts', []):
            network = contract_info['network']
            contract_name = contract_info['name'].replace('.sol', '')
            deployment_file = os.path.join(contracts_path, f"{contract_name}.json")
            if not os.path.exists(deployment_file):
                logger.error("Deployment file %s does not exist.", deployment_file)
                continue
            with open(deployment_file, 'r') as file:
                deployment_data = json.load(file)
            contract_address = deployment_data.get('networks', {}).get(str(self.blockchain_config['networks'][network]['chain_id']), {}).get('address')
            if not contract_address:
                logger.error("Contract address for %s on network %s not found.", contract_name, network)
                continue
            abi = deployment_data['contracts'][f"{contract_name}.sol"][contract_name]['abi']
            web3 = self.web3_connections.get(network)
            if web3:
                contract = web3.eth.contract(address=contract_address, abi=abi)
                contracts[contract_name] = contract
                logger.info("Loaded contract %s at address %s on network %s", contract_name, contract_address, network)
            else:
                logger.error("Web3 connection for network %s not available.", network)
        return contracts

    def distribute_reward(self, network: str, user_address: str, amount: float) -> Optional[str]:
        """
        Distributes reward to a user by invoking the smart contract.
        
        Parameters:
        - network (str): The blockchain network to use.
        - user_address (str): The recipient's wallet address.
        - amount (float): The amount of TNS to distribute.
        
        Returns:
        - str: Transaction hash if successful, else None.
        """
        contract = self.contracts.get('RewardDistributor')
        if not contract:
            logger.error("RewardDistributor contract not loaded.")
            return None
        
        web3 = self.web3_connections.get(network)
        if not web3:
            logger.error("Web3 connection for network %s not available.", network)
            return None

        private_key = os.getenv(f"{network.upper()}_PRIVATE_KEY")
        if not private_key:
            logger.error("Private key for network %s not found in environment variables.", network)
            return None

        account = web3.eth.account.from_key(private_key)
        nonce = web3.eth.get_transaction_count(account.address)

        try:
            tx = contract.functions.distributeReward(user_address, web3.toWei(amount, 'ether')).buildTransaction({
                'from': account.address,
                'nonce': nonce,
                'gas': 200000,
                'gasPrice': web3.toWei(self.blockchain_config['networks'][network]['gas_price'], 'gwei'),
                'chainId': self.blockchain_config['networks'][network]['chain_id']
            })

            signed_tx = web3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info("Sent reward distribution transaction %s to %s on network %s", tx_hash.hex(), user_address, network)
            return tx_hash.hex()
        except ContractLogicError as e:
            logger.error("Smart contract logic error: %s", e)
            return None
        except Exception as e:
            logger.error("Failed to send transaction: %s", e)
            return None

class RewardProcessor:
    """
    Processes incoming AI-processed data and manages reward distribution.
    """
    def __init__(self, config: RewardDistributionConfig):
        self.config = config.config['reward_distribution']
        self.kafka_consumer = KafkaConsumerWrapper(self.config['kafka'])
        self.kafka_producer = KafkaProducerWrapper(self.config['kafka'])
        self.db_manager = DatabaseManager(self.config['database'])
        self.contract_manager = SmartContractManager(self.config['blockchain'], self.db_manager)
        logger.info("RewardProcessor initialized.")

    def process_reward(self, data: Dict[str, Any]):
        """
        Processes a single data entry to determine and distribute rewards.
        
        Parameters:
        - data (dict): The AI-processed data containing user information and metrics.
        """
        try:
            user_address = data.get('user_address')
            if not user_address:
                logger.warning("No user address found in data: %s", data)
                return

            # Determine reward amount based on metrics
            # Example: reward more for higher engagement or positive sentiment
            engagement_score = data.get('engagement_score', 0)
            sentiment = data.get('sentiment', 'neutral')

            if sentiment == 'positive':
                sentiment_multiplier = 1.5
            elif sentiment == 'negative':
                sentiment_multiplier = 0.5
            else:
                sentiment_multiplier = 1.0

            reward_amount = engagement_score * sentiment_multiplier
            reward_amount = round(reward_amount, 2)  # Round to 2 decimal places

            if reward_amount <= 0:
                logger.info("No reward to distribute for user %s based on data: %s", user_address, data)
                return

            # Distribute reward via smart contract
            network = self.config['blockchain']['default_network']
            tx_hash = self.contract_manager.distribute_reward(network, user_address, reward_amount)

            if tx_hash:
                reward_record = {
                    'user_address': user_address,
                    'amount': reward_amount,
                    'tx_hash': tx_hash,
                    'status': 'sent',
                    'created_at': datetime.utcnow().isoformat()
                }
                self.db_manager.insert_reward(reward_record)
                self.kafka_producer.send(reward_record)
                logger.info("Distributed reward of %s TNS to %s with transaction %s", reward_amount, user_address, tx_hash)
            else:
                reward_record = {
                    'user_address': user_address,
                    'amount': reward_amount,
                    'tx_hash': None,
                    'status': 'failed',
                    'created_at': datetime.utcnow().isoformat()
                }
                self.db_manager.insert_reward(reward_record)
                self.kafka_producer.send(reward_record)
                logger.error("Failed to distribute reward to %s", user_address)

        except Exception as e:
            logger.error("Error processing reward for data %s: %s", data, e)

    def run(self):
        """
        Runs the RewardProcessor to consume data and distribute rewards.
        """
        logger.info("Starting RewardProcessor.")
        for message in self.kafka_consumer.consume_messages():
            self.process_reward(message)
        logger.info("RewardProcessor stopped.")

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize configuration
    config = RewardDistributionConfig()

    # Initialize and run RewardProcessor
    processor = RewardProcessor(config)
    processor.run()

if __name__ == "__main__":
    main()

