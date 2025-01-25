# pumpfun_anaylsis.py

import os
import json
import logging
from typing import Dict, Any, Optional, List

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import re

from scanning_patterns import ScanningPatterns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("pumpfun_anaylsis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PumpFunAnalysisConfig:
    """
    Configuration loader for PumpFun Analysis module.
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
            import yaml
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
        required_sections = ['pumpfun_analysis', 'kafka', 'database', 'scanning_patterns']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        pumpfun_config = self.config['pumpfun_analysis']
        required_keys = ['consumer_topic', 'producer_topic', 'analysis_rules']
        for key in required_keys:
            if key not in pumpfun_config:
                logger.error(f"Missing required config parameter in pumpfun_analysis: {key}")
                raise KeyError(f"Missing required config parameter in pumpfun_analysis: {key}")

        logger.info("PumpFunAnalysis configuration validated successfully.")

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
                'pumpfun_analysis', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('network', String(50), nullable=False),
                Column('project_name', String(255), nullable=False),
                Column('price', Float, nullable=False),
                Column('volume', Float, nullable=False),
                Column('launch_date', String(50), nullable=True),
                Column('contract_address', String(42), nullable=True),
                Column('liquidity_locked', String(10), nullable=True),
                Column('audit_status', String(20), nullable=True),
                Column('patterns_matched', String(255), nullable=True),
                Column('analysis_timestamp', DateTime, default=datetime.utcnow)
            )
            self.metadata.create_all(self.engine)
            logger.info("Database connected and pumpfun_analysis table ensured.")
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
                network=analysis.get('network', 'pumpfun'),
                project_name=analysis.get('project_name'),
                price=analysis.get('price', 0.0),
                volume=analysis.get('volume', 0.0),
                launch_date=analysis.get('launch_date'),
                contract_address=analysis.get('contract_address'),
                liquidity_locked=analysis.get('liquidity_locked'),
                audit_status=analysis.get('audit_status'),
                patterns_matched=json.dumps(analysis.get('patterns_matched', [])),
                analysis_timestamp=datetime.utcnow()
            )
            session.execute(ins)
            session.commit()
            logger.debug(f"Inserted analysis for project {analysis.get('project_name')} into database.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to insert analysis for project {analysis.get('project_name')}: {e}")
        finally:
            session.close()

class KafkaProducerWrapper:
    """
    Wrapper for Kafka Producer to send messages to a Kafka topic.
    """

    def __init__(self, kafka_config: Dict[str, Any]):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('producer_topic', 'pumpfun_analysis')
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

class PumpFunAnalyzer:
    """
    Analyzes data from pump.fun for high-potential tokens and potential threats.
    Utilizes pattern matching and rule-based analysis.
    """

    def __init__(self, config: PumpFunAnalysisConfig, db_manager: DatabaseManager, kafka_producer: KafkaProducerWrapper):
        self.config = config.config['pumpfun_analysis']
        self.kafka_producer = kafka_producer
        self.db_manager = db_manager
        self.consumer_topic = self.config['consumer_topic']
        self.analysis_rules = self.config['analysis_rules']
        self.patterns = ScanningPatterns('config/patterns.json')
        logger.info("PumpFunAnalyzer initialized with consumer_topic: %s", self.consumer_topic)

        # Initialize Kafka Consumer
        try:
            self.consumer = KafkaConsumer(
                self.consumer_topic,
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                group_id='pumpfun_analysis_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"Initialized KafkaConsumer for topic: {self.consumer_topic}")
        except KafkaError as e:
            logger.error(f"Failed to initialize KafkaConsumer: {e}")
            raise

    def match_patterns(self, data: Dict[str, Any]) -> List[str]:
        """
        Matches data against predefined patterns.
        
        Parameters:
        - data (Dict[str, Any]): The data to analyze.
        
        Returns:
        - List[str]: List of matched pattern names.
        """
        matched_patterns = []
        analysis_patterns = self.patterns.get_patterns('pumpfun_analysis')
        
        # Example pattern matching
        for pattern in analysis_patterns.get('regex_patterns', []):
            if re.search(pattern, json.dumps(data), re.IGNORECASE):
                matched_patterns.append(pattern)
        
        logger.debug(f"Matched patterns: {matched_patterns}")
        return matched_patterns

    def analyze_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes a single data entry from pump.fun.
        
        Parameters:
        - data (Dict[str, Any]): The data to analyze.
        
        Returns:
        - Dict[str, Any]: Analysis results.
        """
        logger.info(f"Analyzing data for project: {data.get('project_name')}")
        patterns_matched = self.match_patterns(data)
        
        analysis = {
            'network': 'pumpfun',
            'project_name': data.get('project_name'),
            'price': data.get('price', 0.0),
            'volume': data.get('volume', 0.0),
            'launch_date': data.get('launch_date'),
            'contract_address': data.get('contract_address'),
            'liquidity_locked': data.get('liquidity_locked'),
            'audit_status': data.get('audit_status'),
            'patterns_matched': patterns_matched,
            'analysis_timestamp': datetime.utcnow().isoformat()
        }
        
        logger.debug(f"Analysis result: {analysis}")
        return analysis

    def run(self):
        """
        Runs the PumpFunAnalyzer to continuously consume and analyze data.
        """
        logger.info("PumpFunAnalyzer started running.")
        try:
            for message in self.consumer:
                data = message.value
                analysis = self.analyze_data(data)
                if analysis['patterns_matched']:
                    # Store in database
                    self.db_manager.insert_analysis(analysis)
                    # Forward to Kafka
                    self.kafka_producer.send(analysis)
                    logger.info(f"Processed and forwarded analysis for project: {analysis['project_name']}")
                else:
                    logger.info(f"No significant patterns matched for project: {analysis['project_name']}")
        except Exception as e:
            logger.error(f"Error in PumpFunAnalyzer run loop: {e}")
        finally:
            self.consumer.close()
            logger.info("PumpFunAnalyzer stopped.")

def load_configuration(config_path: str = 'config.yaml') -> PumpFunAnalysisConfig:
    """
    Loads the pumpfun analysis configuration.
    """
    config = PumpFunAnalysisConfig(config_path)
    return config

def main():
    """
    Main function to execute the PumpFunAnalyzer.
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

    # Initialize and run PumpFunAnalyzer
    try:
        analyzer = PumpFunAnalyzer(config, db_manager, kafka_producer)
        analyzer.run()
    except Exception as e:
        logger.error(f"PumpFunAnalyzer encountered an error: {e}")

if __name__ == "__main__":
    main()

