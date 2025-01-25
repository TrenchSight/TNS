# twitter_scanning.py

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

import tweepy
from tweepy import StreamListener, Stream, API, OAuthHandler
from dotenv import load_dotenv
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("twitter_scanning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TwitterConfig:
    """
    Configuration for Twitter API and streaming.
    """
    def __init__(self, config_path: str = 'config/twitter_config.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads Twitter configuration from a JSON file.
        """
        if not os.path.exists(self.config_path):
            logger.error("Twitter configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")

        with open(self.config_path, 'r') as file:
            config = json.load(file)
        logger.info("Loaded Twitter configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_keys = [
            'consumer_key', 'consumer_secret', 'access_token',
            'access_token_secret', 'keywords', 'kafka'
        ]
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("Twitter configuration validated successfully.")

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'twitter_stream')
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

class TwitterStreamListener(StreamListener):
    """
    Tweepy StreamListener to handle incoming tweets.
    """
    def __init__(self, kafka_producer: KafkaProducerWrapper):
        super().__init__()
        self.kafka_producer = kafka_producer

    def on_status(self, status):
        """
        Called when a new status arrives.
        """
        tweet_data = self.extract_tweet_data(status)
        if tweet_data:
            self.kafka_producer.send(tweet_data)
            logger.info("Processed tweet ID: %s", tweet_data['id'])

    def on_error(self, status_code):
        """
        Called when a non-200 status code is returned.
        """
        logger.error("Stream encountered error with status code: %s", status_code)
        if status_code == 420:
            # Returning False disconnects the stream
            return False

    def extract_tweet_data(self, status) -> Dict[str, Any]:
        """
        Extracts relevant data from a tweet status object.
        """
        try:
            tweet = {
                'id': status.id_str,
                'created_at': status.created_at.isoformat(),
                'text': status.text,
                'user': {
                    'id': status.user.id_str,
                    'name': status.user.name,
                    'screen_name': status.user.screen_name,
                    'followers_count': status.user.followers_count,
                    'verified': status.user.verified
                },
                'entities': status.entities,
                'lang': status.lang
            }
            logger.debug("Extracted tweet data: %s", tweet)
            return tweet
        except Exception as e:
            logger.error("Failed to extract tweet data: %s", e)
            return {}

class TwitterScanner:
    """
    Main class to handle Twitter streaming and data ingestion.
    """
    def __init__(self, config: TwitterConfig):
        self.config = config
        self.auth = self.authenticate()
        self.api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.kafka_producer = KafkaProducerWrapper(self.config.config['kafka'])
        logger.info("TwitterScanner initialized.")

    def authenticate(self) -> OAuthHandler:
        """
        Authenticates with the Twitter API using OAuth.
        """
        auth = OAuthHandler(self.config.config['consumer_key'], self.config.config['consumer_secret'])
        auth.set_access_token(self.config.config['access_token'], self.config.config['access_token_secret'])
        logger.info("Authenticated with Twitter API.")
        return auth

    def start_stream(self):
        """
        Starts the Twitter stream to listen for specified keywords.
        """
        listener = TwitterStreamListener(self.kafka_producer)
        stream = Stream(auth=self.auth, listener=listener)
        keywords = self.config.config['keywords']
        logger.info("Starting Twitter stream for keywords: %s", keywords)
        try:
            stream.filter(track=keywords, languages=["en"], is_async=False)
        except Exception as e:
            logger.error("Error while streaming tweets: %s", e)

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize Twitter configuration
    twitter_config = TwitterConfig()

    # Initialize and start Twitter Scanner
    scanner = TwitterScanner(twitter_config)
    scanner.start_stream()

if __name__ == "__main__":
    main()

