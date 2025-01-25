# launch_scaling.py

import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional

import yaml
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("launch_scaling.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LaunchScalingConfig:
    """
    Configuration loader for Launch Scaling module.
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
        required_sections = ['launch_scaling', 'kafka', 'kubernetes']
        for section in required_sections:
            if section not in self.config:
                logger.error("Missing required config section: %s", section)
                raise KeyError(f"Missing required config section: {section}")
        
        launch_scaling_config = self.config['launch_scaling']
        required_keys = ['min_agents', 'max_agents', 'scale_up_threshold', 'scale_down_threshold']
        for key in required_keys:
            if key not in launch_scaling_config:
                logger.error("Missing required config parameter in launch_scaling: %s", key)
                raise KeyError(f"Missing required config parameter in launch_scaling: {key}")
        
        logger.info("LaunchScaling configuration validated successfully.")

class KafkaMonitor:
    """
    Monitors Kafka consumer group lag to determine scaling needs.
    """
    def __init__(self, kafka_config: Dict[str, Any], consumer_group: str):
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'token_launches')
        self.consumer_group = consumer_group
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                enable_auto_commit=False
            )
            logger.info("Initialized KafkaConsumer for topic: %s and group: %s", self.topic, self.consumer_group)
        except KafkaError as e:
            logger.error("Failed to initialize KafkaConsumer: %s", e)
            raise

    def get_lag(self) -> int:
        """
        Calculates the total lag for the consumer group.
        """
        try:
            self.consumer.subscribe([self.topic])
            partitions = self.consumer.partitions_for_topic(self.topic)
            if not partitions:
                logger.error("No partitions found for topic: %s", self.topic)
                return 0
            end_offsets = self.consumer.end_offsets([client.TopicPartition(self.topic, p) for p in partitions])
            committed = self.consumer.committed([client.TopicPartition(self.topic, p) for p in partitions])
            total_lag = 0
            for tp in committed:
                if tp in end_offsets:
                    lag = end_offsets[tp] - self.consumer.committed(tp)
                    total_lag += lag
            logger.info("Current total Kafka lag for group %s on topic %s: %d", self.consumer_group, self.topic, total_lag)
            return total_lag
        except KafkaError as e:
            logger.error("Error calculating Kafka lag: %s", e)
            return 0
        finally:
            self.consumer.close()

class KubernetesScaler:
    """
    Handles scaling of Swarm Agents in Kubernetes based on load.
    """
    def __init__(self, kubernetes_config: Dict[str, Any]):
        self.namespace = kubernetes_config.get('namespace', 'default')
        self.deployment_name = kubernetes_config.get('deployment_name', 'swarm-agent')
        try:
            config.load_kube_config()
            self.apps_v1 = client.AppsV1Api()
            logger.info("Initialized Kubernetes client for namespace: %s, deployment: %s", self.namespace, self.deployment_name)
        except Exception as e:
            logger.error("Failed to load Kubernetes configuration: %s", e)
            raise

    def get_current_replicas(self) -> Optional[int]:
        """
        Retrieves the current number of replicas for the deployment.
        """
        try:
            deployment = self.apps_v1.read_namespaced_deployment(self.deployment_name, self.namespace)
            current_replicas = deployment.spec.replicas
            logger.info("Current replicas for deployment %s: %d", self.deployment_name, current_replicas)
            return current_replicas
        except ApiException as e:
            logger.error("API exception when reading deployment: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error when reading deployment: %s", e)
            return None

    def scale_deployment(self, replicas: int):
        """
        Scales the deployment to the specified number of replicas.
        """
        try:
            body = {
                'spec': {
                    'replicas': replicas
                }
            }
            self.apps_v1.patch_namespaced_deployment_scale(
                name=self.deployment_name,
                namespace=self.namespace,
                body=body
            )
            logger.info("Scaled deployment %s to %d replicas.", self.deployment_name, replicas)
        except ApiException as e:
            logger.error("API exception when scaling deployment: %s", e)
        except Exception as e:
            logger.error("Unexpected error when scaling deployment: %s", e)

class LaunchScaler:
    """
    Orchestrates the scaling process based on Kafka lag.
    """
    def __init__(self, config: LaunchScalingConfig):
        self.config = config.config['launch_scaling']
        self.kafka_monitor = KafkaMonitor(
            kafka_config=config.config['kafka'],
            consumer_group=self.config.get('consumer_group', 'swarm_agents_group')
        )
        self.kubernetes_scaler = KubernetesScaler(config.config['kubernetes'])
        self.min_agents = self.config.get('min_agents', 1)
        self.max_agents = self.config.get('max_agents', 10)
        self.scale_up_threshold = self.config.get('scale_up_threshold', 100)
        self.scale_down_threshold = self.config.get('scale_down_threshold', 10)
        logger.info("LaunchScaler initialized with min_agents: %d, max_agents: %d, scale_up_threshold: %d, scale_down_threshold: %d",
                    self.min_agents, self.max_agents, self.scale_up_threshold, self.scale_down_threshold)

    def evaluate_scaling(self, lag: int) -> Optional[int]:
        """
        Determines the desired number of replicas based on Kafka lag.
        """
        current_replicas = self.kubernetes_scaler.get_current_replicas()
        if current_replicas is None:
            logger.error("Unable to retrieve current replicas. Scaling aborted.")
            return None

        desired_replicas = current_replicas

        if lag > self.scale_up_threshold and current_replicas < self.max_agents:
            # Scale up
            scale_factor = (lag // self.scale_up_threshold)
            desired_replicas = min(current_replicas + scale_factor, self.max_agents)
            logger.info("Scaling up: Desired replicas calculated as %d based on lag %d.", desired_replicas, lag)
        elif lag < self.scale_down_threshold and current_replicas > self.min_agents:
            # Scale down
            scale_factor = ((self.scale_down_threshold - lag) // self.scale_down_threshold) + 1
            desired_replicas = max(current_replicas - scale_factor, self.min_agents)
            logger.info("Scaling down: Desired replicas calculated as %d based on lag %d.", desired_replicas, lag)
        else:
            logger.info("No scaling action required. Current replicas: %d, Lag: %d.", current_replicas, lag)
            return None

        if desired_replicas != current_replicas:
            logger.info("Scaling from %d to %d replicas.", current_replicas, desired_replicas)
            return desired_replicas
        else:
            logger.info("Desired replicas (%d) equal to current replicas (%d). No scaling action taken.", desired_replicas, current_replicas)
            return None

    def run_scaling_cycle(self):
        """
        Executes a single scaling evaluation cycle.
        """
        lag = self.kafka_monitor.get_lag()
        desired_replicas = self.evaluate_scaling(lag)
        if desired_replicas is not None:
            self.kubernetes_scaler.scale_deployment(desired_replicas)

    async def run_periodically(self, interval: int = 60):
        """
        Runs the scaling evaluation periodically.
        
        Parameters:
        - interval (int): Time in seconds between scaling evaluations.
        """
        logger.info("Starting periodic scaling evaluations every %d seconds.", interval)
        while True:
            self.run_scaling_cycle()
            await asyncio.sleep(interval)

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize configuration
    config = LaunchScalingConfig()

    # Initialize LaunchScaler
    scaler = LaunchScaler(config)

    # Run the scaling process periodically
    try:
        asyncio.run(scaler.run_periodically(interval=config.config['launch_scaling'].get('evaluation_interval', 60)))
    except KeyboardInterrupt:
        logger.info("Launch scaling interrupted by user.")
    except Exception as e:
        logger.error("An unexpected error occurred in launch scaling: %s", e)

if __name__ == "__main__":
    main()

