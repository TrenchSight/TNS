# deploy_contracts.py

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from web3 import Web3
from solcx import compile_standard, install_solc
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("deploy_contracts.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ContractDeployer:
    """
    A class to compile and deploy smart contracts to EVM-compatible blockchains.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the ContractDeployer with configuration parameters.

        Parameters:
        - config (dict): Configuration parameters for deployment.
        """
        self.config = config
        self.networks = config.get('networks', {})
        self.contracts_path = Path(config.get('contracts_path', 'contracts'))
        self.build_path = Path(config.get('build_path', 'build'))
        self.build_path.mkdir(parents=True, exist_ok=True)
        install_solc(config.get('solc_version', '0.8.17'))
        logger.info("ContractDeployer initialized with config: %s", config)

    def compile_contract(self, contract_name: str) -> Optional[Dict[str, Any]]:
        """
        Compiles a Solidity contract.

        Parameters:
        - contract_name (str): The name of the contract file (e.g., 'MyToken.sol').

        Returns:
        - dict: The compiled contract data.
        """
        contract_file = self.contracts_path / contract_name
        if not contract_file.exists():
            logger.error("Contract file %s does not exist.", contract_file)
            return None

        with open(contract_file, 'r') as file:
            contract_source = file.read()

        logger.info("Compiling contract %s", contract_name)
        try:
            compiled_sol = compile_standard({
                "language": "Solidity",
                "sources": {contract_name: {"content": contract_source}},
                "settings": {
                    "outputSelection": {
                        "*": {
                            "*": ["abi", "metadata", "evm.bytecode", "evm.sourceMap"]
                        }
                    }
                }
            }, solc_version=self.config.get('solc_version', '0.8.17'))

            # Save the compiled contract to the build directory
            output_file = self.build_path / f"{contract_name.replace('.sol', '')}.json"
            with open(output_file, 'w') as outfile:
                json.dump(compiled_sol, outfile, indent=4)
            logger.info("Compiled contract saved to %s", output_file)

            return compiled_sol
        except Exception as e:
            logger.error("Failed to compile contract %s: %s", contract_name, e)
            return None

    def deploy_contract(self, network: str, contract_name: str, constructor_args: list = []) -> Optional[str]:
        """
        Deploys a compiled contract to the specified network.

        Parameters:
        - network (str): The network identifier as defined in the config.
        - contract_name (str): The name of the contract to deploy.
        - constructor_args (list): Arguments for the contract's constructor.

        Returns:
        - str: The address of the deployed contract.
        """
        if network not in self.networks:
            logger.error("Network %s is not configured.", network)
            return None

        network_config = self.networks[network]
        w3 = Web3(Web3.HTTPProvider(network_config['rpc_url']))

        if not w3.isConnected():
            logger.error("Failed to connect to network %s at %s", network, network_config['rpc_url'])
            return None

        private_key = os.getenv(f"{network.upper()}_PRIVATE_KEY")
        if not private_key:
            logger.error("Private key for network %s not found in environment variables.", network)
            return None

        account = w3.eth.account.from_key(private_key)
        w3.eth.default_account = account.address
        logger.info("Using account %s for deployment on network %s", account.address, network)

        compiled_sol = self.compile_contract(contract_name)
        if not compiled_sol:
            logger.error("Compilation failed for contract %s", contract_name)
            return None

        contract_id = contract_name.replace('.sol', '')
        contract_interface = compiled_sol['contracts'][contract_name][contract_id]
        abi = contract_interface['abi']
        bytecode = contract_interface['evm']['bytecode']['object']

        Contract = w3.eth.contract(abi=abi, bytecode=bytecode)

        # Estimate Gas
        try:
            estimated_gas = Contract.constructor(*constructor_args).estimateGas()
            logger.info("Estimated gas for deployment: %s", estimated_gas)
        except Exception as e:
            logger.error("Gas estimation failed for contract %s: %s", contract_name, e)
            return None

        # Build Transaction
        try:
            txn = Contract.constructor(*constructor_args).buildTransaction({
                'from': account.address,
                'nonce': w3.eth.get_transaction_count(account.address),
                'gas': estimated_gas,
                'gasPrice': w3.toWei(network_config.get('gas_price', '50'), 'gwei'),
                'chainId': network_config['chain_id']
            })
            logger.info("Built transaction for contract %s deployment", contract_name)
        except Exception as e:
            logger.error("Failed to build transaction for contract %s: %s", contract_name, e)
            return None

        # Sign Transaction
        try:
            signed_txn = w3.eth.account.sign_transaction(txn, private_key=private_key)
            logger.info("Signed transaction for contract %s deployment", contract_name)
        except Exception as e:
            logger.error("Failed to sign transaction for contract %s: %s", contract_name, e)
            return None

        # Send Transaction
        try:
            tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            logger.info("Sent transaction with hash %s for contract %s deployment", tx_hash.hex(), contract_name)
        except Exception as e:
            logger.error("Failed to send transaction for contract %s: %s", contract_name, e)
            return None

        # Wait for Transaction Receipt
        try:
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
            contract_address = receipt.contractAddress
            logger.info("Contract %s deployed at address %s on network %s", contract_name, contract_address, network)
            return contract_address
        except Exception as e:
            logger.error("Failed to get transaction receipt for contract %s: %s", contract_name, e)
            return None

    def deploy_all_contracts(self):
        """
        Deploys all contracts specified in the configuration to their respective networks.
        """
        contracts = self.config.get('contracts', [])
        for contract in contracts:
            contract_name = contract['name']
            network = contract['network']
            constructor_args = contract.get('constructor_args', [])
            logger.info("Starting deployment of contract %s to network %s", contract_name, network)
            address = self.deploy_contract(network, contract_name, constructor_args)
            if address:
                logger.info("Successfully deployed %s to %s at address %s", contract_name, network, address)
                self.save_deployment_info(contract_name, network, address)
            else:
                logger.error("Failed to deploy contract %s to network %s", contract_name, network)

    def save_deployment_info(self, contract_name: str, network: str, address: str):
        """
        Saves the deployment information to a JSON file.

        Parameters:
        - contract_name (str): The name of the deployed contract.
        - network (str): The network where the contract was deployed.
        - address (str): The address of the deployed contract.
        """
        deployment_file = self.build_path / f"deployments_{network}.json"
        if deployment_file.exists():
            with open(deployment_file, 'r') as file:
                deployments = json.load(file)
        else:
            deployments = {}

        deployments[contract_name] = address

        with open(deployment_file, 'w') as file:
            json.dump(deployments, file, indent=4)
        logger.info("Saved deployment info for contract %s on network %s to %s", contract_name, network, deployment_file)

def load_configuration(config_path: str) -> Dict[str, Any]:
    """
    Loads the deployment configuration from a JSON file.

    Parameters:
    - config_path (str): Path to the configuration file.

    Returns:
    - dict: The configuration dictionary.
    """
    if not os.path.exists(config_path):
        logger.error("Configuration file %s does not exist.", config_path)
        return {}

    with open(config_path, 'r') as file:
        config = json.load(file)
    logger.info("Loaded configuration from %s", config_path)
    return config

def main():
    # Load environment variables
    load_dotenv()

    # Load deployment configuration
    config = load_configuration('deploy_config.json')
    if not config:
        logger.error("Failed to load deployment configuration.")
        return

    # Initialize ContractDeployer
    deployer = ContractDeployer(config)

    # Deploy all contracts
    deployer.deploy_all_contracts()

if __name__ == "__main__":
    main()

