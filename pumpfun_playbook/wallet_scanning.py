# wallet_scanning.py

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

from web3 import Web3
from dotenv import load_dotenv
from kafka import KafkaProducer
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("wallet_scanning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BlockchainConfig:
    """
    Configuration for connecting to blockchain networks.
    """
    def __init__(self, config_path: str = 'config/blockchain_config.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads blockchain configuration from a JSON file.
        """
        if not os.path.exists(self.config_path):
            logger.error("Blockchain configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")
        
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        logger.info("Loaded blockchain configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_keys = ['networks']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("Blockchain configuration validated successfully.")

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'wallet_scans')
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
        except Exception as e:
            logger.error("Failed to send message to Kafka: %s", e)

class WalletScanner:
    """
    Main class to handle wallet scanning and analysis.
    """
    def __init__(self, blockchain_config: BlockchainConfig, kafka_producer: KafkaProducerWrapper):
        self.blockchain_config = blockchain_config
        self.kafka_producer = kafka_producer
        self.web3_connections = self.initialize_web3_connections()
        logger.info("WalletScanner initialized with blockchain networks: %s",
                    list(self.web3_connections.keys()))

    def initialize_web3_connections(self) -> Dict[str, Web3]:
        """
        Initializes Web3 connections for each configured network.
        """
        connections = {}
        for network, params in self.blockchain_config.config['networks'].items():
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

    def fetch_transactions(self, web3: Web3, wallet_address: str, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        """
        Fetches transactions for a given wallet address within a block range.
        """
        logger.info("Fetching transactions for wallet %s from block %d to %d", wallet_address, from_block, to_block)
        transactions = []
        try:
            for block_num in range(from_block, to_block + 1):
                block = web3.eth.get_block(block_num, full_transactions=True)
                for tx in block.transactions:
                    if tx['from'].lower() == wallet_address.lower() or tx['to'].lower() == wallet_address.lower():
                        tx_data = {
                            'hash': tx['hash'].hex(),
                            'from': tx['from'],
                            'to': tx['to'],
                            'value': web3.fromWei(tx['value'], 'ether'),
                            'gas': tx['gas'],
                            'gasPrice': web3.fromWei(tx['gasPrice'], 'gwei'),
                            'nonce': tx['nonce'],
                            'blockNumber': tx['blockNumber'],
                            'timestamp': datetime.utcfromtimestamp(block.timestamp).isoformat()
                        }
                        transactions.append(tx_data)
                if block_num % 1000 == 0:
                    logger.info("Processed up to block %d", block_num)
        except Exception as e:
            logger.error("Error fetching transactions: %s", e)
        logger.info("Fetched %d transactions for wallet %s", len(transactions), wallet_address)
        return transactions

    def analyze_transactions(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyzes transactions to identify suspicious patterns.
        """
        logger.info("Analyzing %d transactions", len(transactions))
        df = pd.DataFrame(transactions)
        analysis = {}

        # Example Analysis: High frequency of transactions in short time
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['time_diff'] = df['timestamp'].diff().dt.total_seconds()
        high_freq = df[df['time_diff'] < 60]  # Transactions within 1 minute
        analysis['high_frequency_transactions'] = high_freq.shape[0]

        # Example Analysis: Large transactions
        large_tx = df[df['value'] > 1000]  # Transactions greater than 1000 ETH
        analysis['large_transactions'] = large_tx.shape[0]

        # Example Analysis: Repeated recipients
        repeated_recipients = df['to'].value_counts()
        repeated_recipients = repeated_recipients[repeated_recipients > 5].to_dict()
        analysis['repeated_recipients'] = repeated_recipients

        logger.info("Analysis results: %s", analysis)
        return analysis

    def scan_wallet(self, network: str, wallet_address: str):
        """
        Scans a single wallet address on a specified network.
        """
        if network not in self.web3_connections:
            logger.error("No Web3 connection found for network %s", network)
            return

        web3 = self.web3_connections[network]
        latest_block = web3.eth.block_number
        lookback_blocks = self.blockchain_config.config['networks'][network].get('lookback_blocks', 10000)
        from_block = max(latest_block - lookback_blocks, 0)
        to_block = latest_block

        transactions = self.fetch_transactions(web3, wallet_address, from_block, to_block)
        analysis = self.analyze_transactions(transactions)

        report = {
            'network': network,
            'wallet_address': wallet_address,
            'latest_block_scanned': to_block,
            'transactions_found': len(transactions),
            'analysis': analysis,
            'timestamp': datetime.utcnow().isoformat()
        }

        self.kafka_producer.send(report)
        logger.info("Scanned wallet %s on network %s and sent report to Kafka", wallet_address, network)

    def scan_wallets(self, wallets: List[Dict[str, str]]):
        """
        Scans multiple wallets across different networks.
        """
        logger.info("Starting scan for %d wallets", len(wallets))
        for wallet in wallets:
            network = wallet.get('network')
            address = wallet.get('address')
            if network and address:
                logger.info("Scanning wallet %s on network %s", address, network)
                self.scan_wallet(network, address)
            else:
                logger.warning("Invalid wallet entry: %s", wallet)

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

    # Initialize Blockchain configuration
    blockchain_config = BlockchainConfig()

    # Initialize Kafka Producer
    kafka_producer = KafkaProducerWrapper(blockchain_config.config.get('kafka', {}))

    # Initialize WalletScanner
    wallet_scanner = WalletScanner(blockchain_config, kafka_producer)

    # Load wallets to scan
    wallets = load_wallets('data/wallets_to_scan.json')
    if not wallets:
        logger.error("No wallets to scan. Exiting.")
        return

    # Start scanning wallets
    wallet_scanner.scan_wallets(wallets)

if __name__ == "__main__":
    main()

