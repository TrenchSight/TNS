# blockchain_anaylsis.py

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from web3 import Web3
from web3.middleware import geth_poa_middleware
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from scanning_patterns import ScanningPatterns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("blockchain_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BlockchainAnalysisConfig:
    """
    Configuration loader for Blockchain Analysis module.
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
        required_sections = ['blockchain', 'kafka', 'database', 'scanning_patterns']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        blockchain_config = self.config['blockchain']
        if 'networks' not in blockchain_config:
            logger.error("Missing 'networks' in blockchain configuration.")
            raise KeyError("Missing 'networks' in blockchain configuration.")

        logger.info("BlockchainAnalysis configuration validated successfully.")

class DatabaseManager:
    """
    Manages database connections and operations for storing analysis results.
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
            self.analysis_table = Table(
                'blockchain_analysis', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('network', String(50), nullable=False),
                Column('transaction_hash', String(66), unique=True, nullable=False),
                Column('from_address', String(42), nullable=False),
                Column('to_address', String(42), nullable=True),
                Column('value', Float, nullable=False),
                Column('gas', Integer, nullable=False),
                Column('gas_price', Float, nullable=False),
                Column('nonce', Integer, nullable=False),
                Column('block_number', Integer, nullable=False),
                Column('timestamp', DateTime, nullable=False),
                Column('patterns_matched', String(255), nullable=True),
                Column('analysis_timestamp', DateTime, default=datetime.utcnow)
            )
            self.metadata.create_all(self.engine)
            logger.info("Database connected and blockchain_analysis table ensured.")
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def insert_analysis(self, analysis: Dict[str, Any]):
        """
        Inserts an analysis record into the database.
        """
        session = self.Session()
        try:
            ins = self.analysis_table.insert().values(
                network=analysis['network'],
                transaction_hash=analysis['transaction_hash'],
                from_address=analysis['from_address'],
                to_address=analysis.get('to_address'),
                value=analysis['value'],
                gas=analysis['gas'],
                gas_price=analysis['gas_price'],
                nonce=analysis['nonce'],
                block_number=analysis['block_number'],
                timestamp=datetime.fromisoformat(analysis['timestamp']),
                patterns_matched=json.dumps(analysis['patterns_matched']),
                analysis_timestamp=datetime.utcnow()
            )
            session.execute(ins)
            session.commit()
            logger.debug(f"Inserted analysis for transaction {analysis['transaction_hash']} into database.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to insert analysis {analysis['transaction_hash']}: {e}")
        finally:
            session.close()

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'blockchain_analysis')
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            logger.info(f"Initialized KafkaProducer for topic: {self.topic}")
        except KafkaError as e:
            logger.error(f"Failed to initialize KafkaProducer: {e}")
            raise

    def send(self, message: Dict[str, Any]):
        """
        Sends a message to the Kafka topic.
        """
        try:
            self.producer.send(self.topic, message)
            self.producer.flush()
            logger.debug(f"Sent message to Kafka topic {self.topic}: {message}")
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")

class BlockchainAnalyzer:
    """
    Analyzes blockchain transactions based on predefined patterns.
    """
    def __init__(self, config: BlockchainAnalysisConfig, db_manager: DatabaseManager, kafka_producer: KafkaProducerWrapper):
        self.config = config.config['blockchain']
        self.networks = self.config['networks']
        self.db_manager = db_manager
        self.kafka_producer = kafka_producer
        self.web3_connections = self.initialize_web3_connections()
        self.patterns = ScanningPatterns('config/patterns.json')
        logger.info("BlockchainAnalyzer initialized with networks: %s", list(self.networks.keys()))

    def initialize_web3_connections(self) -> Dict[str, Web3]:
        """
        Initializes Web3 connections for each configured network.
        """
        connections = {}
        for network, params in self.networks.items():
            rpc_url = params.get('rpc_url')
            if not rpc_url:
                logger.error(f"RPC URL not provided for network {network}. Skipping connection.")
                continue
            try:
                web3 = Web3(Web3.HTTPProvider(rpc_url))
                if params.get('is_poa', False):
                    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if web3.isConnected():
                    connections[network] = web3
                    logger.info(f"Connected to network {network} via {rpc_url}")
                else:
                    logger.error(f"Failed to connect to network {network} via {rpc_url}")
            except Exception as e:
                logger.error(f"Exception occurred while connecting to network {network}: {e}")
        return connections

    def fetch_latest_block(self, web3: Web3) -> int:
        """
        Fetches the latest block number from the blockchain.
        """
        try:
            latest_block = web3.eth.block_number
            logger.info(f"Latest block on network: {latest_block}")
            return latest_block
        except Exception as e:
            logger.error(f"Failed to fetch latest block: {e}")
            return 0

    def analyze_transaction(self, network: str, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Analyzes a single transaction for patterns.
        """
        web3 = self.web3_connections.get(network)
        if not web3:
            logger.error(f"No Web3 connection available for network {network}")
            return None

        try:
            tx = web3.eth.get_transaction(tx_hash)
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            block = web3.eth.get_block(tx.blockNumber)
            timestamp = datetime.utcfromtimestamp(block.timestamp).isoformat()

            analysis = {
                'network': network,
                'transaction_hash': tx_hash,
                'from_address': tx['from'],
                'to_address': tx['to'],
                'value': float(web3.fromWei(tx['value'], 'ether')),
                'gas': tx['gas'],
                'gas_price': float(web3.fromWei(tx['gasPrice'], 'gwei')),
                'nonce': tx['nonce'],
                'block_number': tx['blockNumber'],
                'timestamp': timestamp,
                'patterns_matched': []
            }

            # Analyze patterns
            if receipt['status'] == 0:
                analysis['patterns_matched'].append('failed_transaction')

            # Fetch internal transactions if any (requires additional tools or APIs)
            # Placeholder for internal transaction analysis

            # Pattern Matching based on patterns.json
            tx_input = tx['input']
            decoded_input = self.decode_input(tx_input, web3, network)
            if decoded_input:
                logger.debug(f"Decoded input for transaction {tx_hash}: {decoded_input}")

            # Example: Detect if transaction interacts with known malicious contracts
            # Placeholder for specific pattern detections

            # Use ScanningPatterns to match regex patterns
            matched_patterns = self.patterns.get_patterns('blockchain_anaylsis')
            for pattern_name, pattern_details in matched_patterns.items():
                for pattern in pattern_details.get('regex_patterns', []):
                    if re.search(pattern, tx_input, re.IGNORECASE):
                        analysis['patterns_matched'].append(pattern_name)

            if analysis['patterns_matched']:
                logger.info(f"Transaction {tx_hash} matched patterns: {analysis['patterns_matched']}")
            else:
                logger.info(f"Transaction {tx_hash} did not match any patterns.")

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing transaction {tx_hash}: {e}")
            return None

    def decode_input(self, tx_input: str, web3: Web3, network: str) -> Optional[Dict[str, Any]]:
        """
        Decodes the input data of a transaction.
        """
        try:
            # Example: Decode ERC20 transfer function
            abi_path = self.config['contracts_path']
            # This requires knowing which contract is being interacted with
            # Placeholder for decoding logic
            return {}
        except Exception as e:
            logger.error(f"Failed to decode input for transaction: {e}")
            return None

    def scan_blocks(self):
        """
        Scans recent blocks for transactions and analyzes them.
        """
        for network, web3 in self.web3_connections.items():
            latest_block = self.fetch_latest_block(web3)
            start_block = max(latest_block - self.config.get('scan_depth', 10), 0)
            logger.info(f"Scanning blocks {start_block} to {latest_block} on network {network}")

            for block_number in range(start_block, latest_block + 1):
                try:
                    block = web3.eth.get_block(block_number, full_transactions=True)
                    for tx in block.transactions:
                        analysis = self.analyze_transaction(network, tx.hash.hex())
                        if analysis and analysis['patterns_matched']:
                            self.db_manager.insert_analysis(analysis)
                            self.kafka_producer.send(analysis)
                except Exception as e:
                    logger.error(f"Error scanning block {block_number} on network {network}: {e}")

    def run(self):
        """
        Runs the blockchain analysis process.
        """
        logger.info("Starting BlockchainAnalyzer run cycle.")
        self.scan_blocks()
        logger.info("BlockchainAnalyzer run cycle completed.")

def load_configuration(config_path: str = 'config.yaml') -> BlockchainAnalysisConfig:
    """
    Loads the blockchain analysis configuration.
    """
    config = BlockchainAnalysisConfig(config_path)
    return config

def main():
    """
    Main function to execute the blockchain analysis.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load configuration
    try:
        config = load_configuration()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Initialize DatabaseManager
    try:
        db_manager = DatabaseManager(config.config['database'])
    except Exception as e:
        logger.error(f"Failed to initialize DatabaseManager: {e}")
        return

    # Initialize KafkaProducer
    try:
        kafka_producer = KafkaProducerWrapper(config.config['kafka'])
    except Exception as e:
        logger.error(f"Failed to initialize KafkaProducer: {e}")
        return

    # Initialize and run BlockchainAnalyzer
    try:
        analyzer = BlockchainAnalyzer(config, db_manager, kafka_producer)
        analyzer.run()
    except Exception as e:
        logger.error(f"BlockchainAnalyzer encountered an error: {e}")

if __name__ == "__main__":
    main()

