# telegram_scanning.py

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer
from telethon import TelegramClient, events, errors
from telethon.tl.types import PeerChannel
from telethon.errors import FloodWaitError, SessionPasswordNeededError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("telegram_scanning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramConfig:
    """
    Configuration for Telegram API and channel monitoring.
    """
    def __init__(self, config_path: str = 'config/telegram_config.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads Telegram configuration from a JSON file.
        """
        if not os.path.exists(self.config_path):
            logger.error("Telegram configuration file %s does not exist.", self.config_path)
            raise FileNotFoundError(f"Configuration file {self.config_path} not found.")

        with open(self.config_path, 'r') as file:
            config = json.load(file)
        logger.info("Loaded Telegram configuration from %s", self.config_path)
        return config

    def validate_config(self):
        """
        Validates the necessary configuration parameters.
        """
        required_keys = [
            'api_id', 'api_hash', 'bot_token',
            'channels', 'kafka', 'processing_rules'
        ]
        for key in required_keys:
            if key not in self.config:
                logger.error("Missing required config parameter: %s", key)
                raise KeyError(f"Missing required config parameter: {key}")
        logger.info("Telegram configuration validated successfully.")

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'telegram_stream')
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
        except Exception as e:
            logger.error("Failed to send message to Kafka: %s", e)

class TelegramMessageHandler:
    """
    Handles incoming Telegram messages, processes them, and sends to Kafka.
    """
    def __init__(self, processing_rules: Dict[str, Any], kafka_producer: KafkaProducerWrapper):
        self.processing_rules = processing_rules
        self.kafka_producer = kafka_producer

    def process_message(self, message: str, channel: str) -> Dict[str, Any]:
        """
        Processes a Telegram message based on processing rules.

        Parameters:
        - message (str): The content of the Telegram message.
        - channel (str): The channel from which the message was received.

        Returns:
        - dict: The processed data to be sent to Kafka.
        """
        logger.debug("Processing message from channel %s: %s", channel, message)
        processed_data = {
            'channel': channel,
            'raw_message': message,
            'processed_at': datetime.utcnow().isoformat()
        }

        # Apply processing rules
        for key, rule in self.processing_rules.items():
            if rule['type'] == 'regex':
                import re
                match = re.search(rule['pattern'], message)
                processed_data[key] = match.group(1) if match else None
            elif rule['type'] == 'keyword':
                processed_data[key] = rule['keyword'] in message.lower()
            elif rule['type'] == 'json_extract':
                import json
                try:
                    json_data = json.loads(message)
                    processed_data[key] = json_data.get(rule['key'], None)
                except json.JSONDecodeError:
                    processed_data[key] = None
            else:
                processed_data[key] = None
        logger.debug("Processed data: %s", processed_data)
        return processed_data

    def handle_event(self, event, channel_name: str):
        """
        Event handler for new Telegram messages.

        Parameters:
        - event: The Telethon event object.
        - channel_name (str): The name of the channel.
        """
        try:
            message = event.message.message
            if not message:
                return
            processed = self.process_message(message, channel_name)
            self.kafka_producer.send(processed)
            logger.info("Processed and sent message from channel %s", channel_name)
        except Exception as e:
            logger.error("Error processing message from channel %s: %s", channel_name, e)

class TelegramScanner:
    """
    Main class to handle Telegram streaming and data ingestion.
    """
    def __init__(self, config: TelegramConfig, kafka_producer: KafkaProducerWrapper):
        self.config = config
        self.kafka_producer = kafka_producer
        self.processing_rules = self.config.config.get('processing_rules', {})
        self.channels = self.config.config.get('channels', [])
        self.client = TelegramClient('trenchsight_session', 
                                     self.config.config['api_id'], 
                                     self.config.config['api_hash'])
        self.message_handler = TelegramMessageHandler(self.processing_rules, self.kafka_producer)
        logger.info("TelegramScanner initialized with %d channels to monitor.", len(self.channels))

    async def start(self):
        """
        Starts the Telegram client and begins listening to specified channels.
        """
        await self.client.start(bot_token=self.config.config['bot_token'])
        logger.info("Telegram client started and authenticated.")

        for channel in self.channels:
            try:
                entity = await self.client.get_entity(channel)
                self.client.add_event_handler(
                    lambda event, ch=channel: self.message_handler.handle_event(event, ch),
                    events.NewMessage(chats=entity)
                )
                logger.info("Listening to channel: %s", channel)
            except Exception as e:
                logger.error("Failed to add event handler for channel %s: %s", channel, e)

        logger.info("TelegramScanner is now listening to all channels.")
        await self.client.run_until_disconnected()

def load_channels(channels_path: str) -> List[str]:
    """
    Loads channel names to monitor from a JSON file.

    Parameters:
    - channels_path (str): Path to the channels JSON file.

    Returns:
    - list of str: List of channel usernames or IDs.
    """
    if not os.path.exists(channels_path):
        logger.error("Channels file %s does not exist.", channels_path)
        return []

    with open(channels_path, 'r') as file:
        channels = json.load(file)
    logger.info("Loaded %d channels from %s", len(channels), channels_path)
    return channels

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize Telegram configuration
    telegram_config = TelegramConfig()

    # Initialize Kafka Producer
    kafka_producer = KafkaProducerWrapper(telegram_config.config.get('kafka', {}))

    # Load channels to monitor
    channels = telegram_config.config.get('channels', [])
    if not channels:
        logger.error("No channels configured to monitor. Exiting.")
        return

    # Initialize TelegramScanner
    scanner = TelegramScanner(telegram_config, kafka_producer)

    # Start Telegram scanning
    import asyncio
    try:
        asyncio.run(scanner.start())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Telegram scanning interrupted by user.")
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)

if __name__ == "__main__":
    main()

