# decentralized_storage.py

import os
import json
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path

import ipfshttpclient
import arweave
from dotenv import load_dotenv
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("decentralized_storage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DecentralizedStorageConfig:
    """
    Configuration loader for Decentralized Storage module.
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
        required_keys = ['decentralized_storage']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config section: %s", key)
                raise KeyError(f"Missing required config section: {key}")
        
        storage_config = self.config['decentralized_storage']
        if 'ipfs' not in storage_config and 'arweave' not in storage_config:
            logger.error("At least one decentralized storage backend ('ipfs' or 'arweave') must be configured.")
            raise KeyError("At least one decentralized storage backend ('ipfs' or 'arweave') must be configured.")
        
        logger.info("DecentralizedStorage configuration validated successfully.")

class IPFSClient:
    """
    Client to interact with IPFS for decentralized storage.
    """
    def __init__(self, ipfs_config: Dict[str, Any]):
        self.ipfs_address = ipfs_config.get('address', '/dns/localhost/tcp/5001/http')
        try:
            self.client = ipfshttpclient.connect(self.ipfs_address)
            logger.info("Connected to IPFS at %s", self.ipfs_address)
        except Exception as e:
            logger.error("Failed to connect to IPFS at %s: %s", self.ipfs_address, e)
            raise ConnectionError(f"Failed to connect to IPFS at {self.ipfs_address}: {e}")

    def upload_file(self, file_path: str, pin: bool = True) -> Optional[str]:
        """
        Uploads a file to IPFS.

        Parameters:
        - file_path (str): Path to the file to upload.
        - pin (bool): Whether to pin the file on IPFS.

        Returns:
        - str: The IPFS hash of the uploaded file.
        """
        try:
            res = self.client.add(file_path, pin=pin)
            ipfs_hash = res['Hash']
            logger.info("Uploaded file %s to IPFS with hash %s", file_path, ipfs_hash)
            return ipfs_hash
        except Exception as e:
            logger.error("Failed to upload file %s to IPFS: %s", file_path, e)
            return None

    def download_file(self, ipfs_hash: str, destination: str) -> bool:
        """
        Downloads a file from IPFS.

        Parameters:
        - ipfs_hash (str): The IPFS hash of the file to download.
        - destination (str): Path to save the downloaded file.

        Returns:
        - bool: True if download was successful, else False.
        """
        try:
            self.client.get(ipfs_hash, target=destination)
            logger.info("Downloaded IPFS hash %s to %s", ipfs_hash, destination)
            return True
        except Exception as e:
            logger.error("Failed to download IPFS hash %s: %s", ipfs_hash, e)
            return False

class ArweaveClient:
    """
    Client to interact with Arweave for decentralized storage.
    """
    def __init__(self, arweave_config: Dict[str, Any]):
        self.arweave_host = arweave_config.get('host', 'arweave.net')
        self.arweave_port = arweave_config.get('port', 443)
        self.arweave_protocol = arweave_config.get('protocol', 'https')
        self.wallet_path = arweave_config.get('wallet_path')
        
        if not self.wallet_path or not os.path.exists(self.wallet_path):
            logger.error("Arweave wallet file %s does not exist.", self.wallet_path)
            raise FileNotFoundError(f"Arweave wallet file {self.wallet_path} not found.")
        
        try:
            with open(self.wallet_path, 'r') as wallet_file:
                wallet_data = json.load(wallet_file)
            self.client = arweave.Client(
                host=self.arweave_host,
                port=self.arweave_port,
                protocol=self.arweave_protocol,
                wallet=wallet_data
            )
            logger.info("Initialized Arweave client with wallet %s", self.wallet_path)
        except Exception as e:
            logger.error("Failed to initialize Arweave client: %s", e)
            raise ConnectionError(f"Failed to initialize Arweave client: {e}")

    def upload_file(self, file_path: str, tags: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Uploads a file to Arweave.

        Parameters:
        - file_path (str): Path to the file to upload.
        - tags (dict): Optional metadata tags.

        Returns:
        - str: The transaction ID of the uploaded file.
        """
        try:
            transaction = self.client.transactions.prepare(
                data=open(file_path, 'rb').read()
            )
            if tags:
                for key, value in tags.items():
                    transaction.tags.add(arweave.Tag(key, value))
            self.client.transactions.sign(transaction)
            self.client.transactions.post(transaction)
            tx_id = transaction.id
            logger.info("Uploaded file %s to Arweave with transaction ID %s", file_path, tx_id)
            return tx_id
        except Exception as e:
            logger.error("Failed to upload file %s to Arweave: %s", file_path, e)
            return None

    def download_file(self, tx_id: str, destination: str) -> bool:
        """
        Downloads a file from Arweave.

        Parameters:
        - tx_id (str): The transaction ID of the file to download.
        - destination (str): Path to save the downloaded file.

        Returns:
        - bool: True if download was successful, else False.
        """
        try:
            data = self.client.transactions.get_data(tx_id, decode=True, string=False)
            with open(destination, 'wb') as f:
                f.write(data)
            logger.info("Downloaded Arweave transaction %s to %s", tx_id, destination)
            return True
        except Exception as e:
            logger.error("Failed to download Arweave transaction %s: %s", tx_id, e)
            return False

class StorageManager:
    """
    Manages interactions with decentralized storage backends (IPFS and Arweave).
    """
    def __init__(self, config: DecentralizedStorageConfig):
        self.config = config.config['decentralized_storage']
        self.ipfs_client = None
        self.arweave_client = None
        
        # Initialize IPFS client if configured
        if 'ipfs' in self.config:
            self.ipfs_client = IPFSClient(self.config['ipfs'])
        
        # Initialize Arweave client if configured
        if 'arweave' in self.config:
            self.arweave_client = ArweaveClient(self.config['arweave'])
        
        logger.info("StorageManager initialized with IPFS: %s and Arweave: %s",
                    bool(self.ipfs_client), bool(self.arweave_client))
    
    def upload_file(self, file_path: str, backend: str = 'ipfs', tags: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Uploads a file to the specified decentralized storage backend.

        Parameters:
        - file_path (str): Path to the file to upload.
        - backend (str): 'ipfs' or 'arweave'.
        - tags (dict): Optional metadata tags (only for Arweave).

        Returns:
        - str: The identifier (hash or transaction ID) of the uploaded file.
        """
        if backend == 'ipfs' and self.ipfs_client:
            return self.ipfs_client.upload_file(file_path)
        elif backend == 'arweave' and self.arweave_client:
            return self.arweave_client.upload_file(file_path, tags)
        else:
            logger.error("Unsupported backend '%s' or backend not configured.", backend)
            return None

    def download_file(self, identifier: str, backend: str = 'ipfs', destination: str = '.') -> bool:
        """
        Downloads a file from the specified decentralized storage backend.

        Parameters:
        - identifier (str): The identifier (hash or transaction ID) of the file to download.
        - backend (str): 'ipfs' or 'arweave'.
        - destination (str): Path to save the downloaded file.

        Returns:
        - bool: True if download was successful, else False.
        """
        if backend == 'ipfs' and self.ipfs_client:
            return self.ipfs_client.download_file(identifier, destination)
        elif backend == 'arweave' and self.arweave_client:
            return self.arweave_client.download_file(identifier, destination)
        else:
            logger.error("Unsupported backend '%s' or backend not configured.", backend)
            return False

    def pin_file_ipfs(self, ipfs_hash: str) -> bool:
        """
        Pins a file on IPFS to ensure it remains available.

        Parameters:
        - ipfs_hash (str): The IPFS hash of the file to pin.

        Returns:
        - bool: True if pinning was successful, else False.
        """
        if self.ipfs_client:
            try:
                self.ipfs_client.client.pin.add(ipfs_hash)
                logger.info("Pinned IPFS hash %s successfully.", ipfs_hash)
                return True
            except Exception as e:
                logger.error("Failed to pin IPFS hash %s: %s", ipfs_hash, e)
                return False
        else:
            logger.error("IPFS client not configured.")
            return False

    def unpin_file_ipfs(self, ipfs_hash: str) -> bool:
        """
        Unpins a file on IPFS.

        Parameters:
        - ipfs_hash (str): The IPFS hash of the file to unpin.

        Returns:
        - bool: True if unpinning was successful, else False.
        """
        if self.ipfs_client:
            try:
                self.ipfs_client.client.pin.rm(ipfs_hash)
                logger.info("Unpinned IPFS hash %s successfully.", ipfs_hash)
                return True
            except Exception as e:
                logger.error("Failed to unpin IPFS hash %s: %s", ipfs_hash, e)
                return False
        else:
            logger.error("IPFS client not configured.")
            return False

def load_configuration(config_path: str = 'config.yaml') -> DecentralizedStorageConfig:
    """
    Loads the decentralized storage configuration.

    Parameters:
    - config_path (str): Path to the configuration file.

    Returns:
    - DecentralizedStorageConfig: The loaded configuration object.
    """
    config = DecentralizedStorageConfig(config_path)
    return config

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Load decentralized storage configuration
    config = load_configuration()

    # Initialize StorageManager
    storage_manager = StorageManager(config)

    # Example usage
    # Upload a file to IPFS
    ipfs_hash = storage_manager.upload_file('data/sample_data.csv', backend='ipfs')
    if ipfs_hash:
        logger.info("File uploaded to IPFS with hash: %s", ipfs_hash)
    
    # Upload a file to Arweave with tags
    arweave_tx_id = storage_manager.upload_file(
        'data/sample_data.csv',
        backend='arweave',
        tags={
            'Project': 'TrenchSight',
            'Type': 'Data',
            'Description': 'Sample data for testing decentralized storage.'
        }
    )
    if arweave_tx_id:
        logger.info("File uploaded to Arweave with transaction ID: %s", arweave_tx_id)
    
    # Download a file from IPFS
    if ipfs_hash:
        success = storage_manager.download_file(ipfs_hash, backend='ipfs', destination='downloads/ipfs_sample_data.csv')
        if success:
            logger.info("File downloaded from IPFS successfully.")
    
    # Download a file from Arweave
    if arweave_tx_id:
        success = storage_manager.download_file(arweave_tx_id, backend='arweave', destination='downloads/arweave_sample_data.csv')
        if success:
            logger.info("File downloaded from Arweave successfully.")

if __name__ == "__main__":
    main()

