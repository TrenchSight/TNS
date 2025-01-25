# scanning_patterns.py

import os
import json
import logging
from typing import Dict, Any, List, Optional

class ScanningPatterns:
    """
    A singleton class to load and provide access to scanning patterns defined in patterns.json.
    """

    _instance = None

    def __new__(cls, patterns_path: str = 'config/patterns.json'):
        if cls._instance is None:
            cls._instance = super(ScanningPatterns, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, patterns_path: str = 'config/patterns.json'):
        if self._initialized:
            return
        self.patterns_path = patterns_path
        self.patterns = {}
        self.load_patterns()
        self._initialized = True

    def load_patterns(self):
        """
        Loads patterns from the specified JSON file.
        """
        if not os.path.exists(self.patterns_path):
            logging.error("Patterns file %s does not exist.", self.patterns_path)
            raise FileNotFoundError(f"Patterns file {self.patterns_path} not found.")

        try:
            with open(self.patterns_path, 'r') as file:
                self.patterns = json.load(file)
            logging.info("Loaded scanning patterns from %s", self.patterns_path)
        except json.JSONDecodeError as e:
            logging.error("Failed to parse patterns file %s: %s", self.patterns_path, e)
            raise

    def get_patterns(self, module: str) -> Dict[str, Any]:
        """
        Retrieves patterns for a specific scanning module.

        Parameters:
        - module (str): The name of the scanning module.

        Returns:
        - dict: The patterns associated with the module.
        """
        patterns = self.patterns.get(module)
        if not patterns:
            logging.warning("No patterns found for module: %s", module)
            return {}
        return patterns

    def get_all_modules(self) -> List[str]:
        """
        Retrieves a list of all modules with defined patterns.

        Returns:
        - list of str: Module names.
        """
        return list(self.patterns.keys())

    def validate_patterns(self) -> bool:
        """
        Validates the structure of the loaded patterns.

        Returns:
        - bool: True if patterns are valid, False otherwise.
        """
        required_top_level_keys = [
            'twitter_scanning',
            'telegram_scanning',
            'web_scanning',
            'wallet_scanning',
            'transaction_patterns',
            'smart_contract_patterns',
            'ai_model_features'
        ]

        for key in required_top_level_keys:
            if key not in self.patterns:
                logging.error("Missing top-level pattern key: %s", key)
                return False

        # Additional validation can be added here as needed
        logging.info("All required pattern sections are present.")
        return True

    def refresh_patterns(self):
        """
        Reloads the patterns from the JSON file.
        """
        self.load_patterns()
        if self.validate_patterns():
            logging.info("Patterns refreshed and validated successfully.")
        else:
            logging.error("Patterns refresh failed due to validation errors.")

def main():
    """
    Example usage of the ScanningPatterns class.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler("scanning_patterns.log"),
            logging.StreamHandler()
        ]
    )

    try:
        scanner_patterns = ScanningPatterns()
        if scanner_patterns.validate_patterns():
            twitter_patterns = scanner_patterns.get_patterns('twitter_scanning')
            print("Twitter Scanning Patterns:", twitter_patterns)

            telegram_patterns = scanner_patterns.get_patterns('telegram_scanning')
            print("Telegram Scanning Patterns:", telegram_patterns)

            # Add more module pattern retrievals as needed
        else:
            logging.error("Pattern validation failed.")
    except Exception as e:
        logging.error("An error occurred while loading scanning patterns: %s", e)

if __name__ == "__main__":
    main()

