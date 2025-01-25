# web_scanning.py

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("web_scanning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WebScanningConfig:
    """
    Configuration for web scanning module.
    """
    def __init__(self, config_path: str = 'config/web_scanning_config.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads web scanning configuration from a JSON file.
        """
        if not os.path.exists(self.config_path):
            logger.error("Web scanning configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")

        with open(self.config_path, 'r') as file:
            config = json.load(file)
        logger.info("Loaded web scanning configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_keys = ['websites', 'kafka', 'scraping', 'selenium']
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("Web scanning configuration validated successfully.")

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'web_scans')
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

class WebScraper:
    """
    A class to scrape web pages for cryptocurrency project data.
    """
    def __init__(self, scraping_config: Dict[str, Any], selenium_config: Dict[str, Any]):
        self.scraping_config = scraping_config
        self.selenium_config = selenium_config
        self.use_selenium = selenium_config.get('enable', False)
        self.driver = self.initialize_webdriver() if self.use_selenium else None

    def initialize_webdriver(self) -> webdriver.Chrome:
        """
        Initializes a Selenium WebDriver instance.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver_path = self.selenium_config.get('driver_path', 'chromedriver')
        try:
            driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)
            logger.info("Initialized Selenium WebDriver.")
            return driver
        except WebDriverException as e:
            logger.error("Failed to initialize Selenium WebDriver: %s", e)
            raise

    def fetch_page(self, url: str) -> str:
        """
        Fetches the content of a web page.
        """
        try:
            if self.use_selenium:
                self.driver.get(url)
                time.sleep(self.selenium_config.get('wait_time', 3))  # Wait for JavaScript to load
                content = self.driver.page_source
                logger.debug("Fetched page content using Selenium for URL: %s", url)
            else:
                response = requests.get(url, headers=self.scraping_config.get('headers', {}), timeout=10)
                response.raise_for_status()
                content = response.text
                logger.debug("Fetched page content using requests for URL: %s", url)
            return content
        except Exception as e:
            logger.error("Failed to fetch page content from %s: %s", url, e)
            return ""

    def parse_page(self, content: str, parse_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the web page content based on provided parsing rules.
        """
        try:
            soup = BeautifulSoup(content, 'html.parser')
            data = {}
            for key, rule in parse_rules.items():
                element = soup.select_one(rule['selector'])
                if element:
                    data[key] = element.get_text(strip=True)
                else:
                    data[key] = None
            logger.debug("Parsed data: %s", data)
            return data
        except Exception as e:
            logger.error("Failed to parse page content: %s", e)
            return {}

    def scrape_website(self, website: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scrapes a single website based on its configuration.
        """
        url = website.get('url')
        parse_rules = website.get('parse_rules', {})
        pagination = website.get('pagination', {})
        results = []

        if not url or not parse_rules:
            logger.warning("Website configuration missing 'url' or 'parse_rules'. Skipping.")
            return results

        pages = pagination.get('pages', 1)
        for page in range(1, pages + 1):
            paginated_url = url.format(page=page) if '{}' in url else url
            logger.info("Scraping URL: %s", paginated_url)
            content = self.fetch_page(paginated_url)
            if not content:
                continue
            parsed_data = self.parse_page(content, parse_rules)
            if parsed_data:
                parsed_data['source_url'] = paginated_url
                parsed_data['scraped_at'] = datetime.utcnow().isoformat()
                results.append(parsed_data)
        logger.info("Scraped %d entries from website: %s", len(results), url)
        return results

    def close(self):
        """
        Closes the Selenium WebDriver if it's initialized.
        """
        if self.driver:
            self.driver.quit()
            logger.info("Closed Selenium WebDriver.")

class WebScanningManager:
    """
    Manager class to handle the web scanning process.
    """
    def __init__(self, config: WebScanningConfig, kafka_producer: KafkaProducerWrapper):
        self.config = config
        self.kafka_producer = kafka_producer
        self.scraper = WebScraper(
            scraping_config=self.config.config.get('scraping', {}),
            selenium_config=self.config.config.get('selenium', {})
        )
        self.websites = self.config.config.get('websites', [])
        logger.info("WebScanningManager initialized with %d websites to scrape.", len(self.websites))

    def scan_websites(self):
        """
        Scans all configured websites and sends the scraped data to Kafka.
        """
        logger.info("Starting web scanning process.")
        for website in self.websites:
            try:
                scraped_data = self.scraper.scrape_website(website)
                for data in scraped_data:
                    self.kafka_producer.send(data)
                logger.info("Sent %d records from website: %s to Kafka.", len(scraped_data), website.get('url'))
            except Exception as e:
                logger.error("Error scanning website %s: %s", website.get('url'), e)
        logger.info("Web scanning process completed.")

    def shutdown(self):
        """
        Shuts down the web scanning manager gracefully.
        """
        self.scraper.close()
        logger.info("WebScanningManager shutdown completed.")

def load_websites(websites_path: str) -> List[Dict[str, Any]]:
    """
    Loads website configurations to scrape from a JSON file.
    """
    if not os.path.exists(websites_path):
        logger.error("Websites file %s does not exist.", websites_path)
        return []

    with open(websites_path, 'r') as file:
        websites = json.load(file)
    logger.info("Loaded %d websites from %s", len(websites), websites_path)
    return websites

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize web scanning configuration
    config = WebScanningConfig()

    # Initialize Kafka Producer
    kafka_producer = KafkaProducerWrapper(config.config.get('kafka', {}))

    # Initialize WebScanningManager
    manager = WebScanningManager(config, kafka_producer)

    try:
        # Load websites to scrape
        websites = load_websites('data/websites_to_scrape.json')
        if not websites:
            logger.error("No websites to scrape. Exiting.")
            return

        # Update manager with loaded websites
        manager.websites = websites

        # Start scanning websites
        manager.scan_websites()

    except KeyboardInterrupt:
        logger.info("Web scanning interrupted by user.")
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
    finally:
        manager.shutdown()

if __name__ == "__main__":
    main()

