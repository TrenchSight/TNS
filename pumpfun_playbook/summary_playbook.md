# TrenchSight Deployment and Operation Playbook

## Table of Contents
1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Smart Contract Deployment](#smart-contract-deployment)
6. [Running the Modules](#running-the-modules)
    - [Data Cleaning](#data-cleaning)
    - [Twitter Scanning](#twitter-scanning)
    - [Wallet Scanning](#wallet-scanning)
    - [Web Scanning](#web-scanning)
    - [Telegram Scanning](#telegram-scanning)
    - [Bundle Scanning](#bundle-scanning)
7. [Monitoring and Logging](#monitoring-and-logging)
8. [Security Considerations](#security-considerations)
9. [Troubleshooting](#troubleshooting)
10. [Contributing](#contributing)
11. [License](#license)

## Introduction
Welcome to the TrenchSight Deployment and Operation Playbook. This guide provides comprehensive instructions to set up, configure, deploy, and operate the TrenchSight platform, an AI-driven solution for monitoring and analyzing cryptocurrency projects to identify high-potential tokens and mitigate risks such as rug pulls.

## Prerequisites
Before proceeding, ensure you have the following:

- **Operating System:** Ubuntu 20.04 LTS or later
- **Python:** Version 3.8 or higher
- **Docker:** Installed and running
- **Docker Compose:** Installed
- **Kafka:** Installed and configured
- **Solidity Compiler:** `solc` version 0.8.17
- **Chromedriver:** Compatible with your Chrome version (if using Selenium)
- **Telegram API Credentials:** `api_id`, `api_hash`, and `bot_token`
- **Twitter API Credentials:** `consumer_key`, `consumer_secret`, `access_token`, `access_token_secret`
- **AI Service API Key:** For processing aggregated data

## Installation

1. **Clone the Repository**
    ```bash
    git clone https://github.com/yourusername/trenchsight.git
    cd trenchsight
    ```

2. **Set Up Virtual Environment**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4. **Install Solidity Compiler**
    ```bash
    pip install py-solc-x
    python -c "from solcx import install_solc; install_solc('0.8.17')"
    ```

5. **Set Up Kafka**
    - Follow [Kafka Installation Guide](https://kafka.apache.org/quickstart) to install and start Kafka and Zookeeper.

## Configuration

1. **Update Configuration File**
    - Edit `config.yaml` with your specific settings.
    - Ensure all API keys, RPC URLs, and other sensitive information are correctly provided.

2. **Environment Variables**
    - Create a `.env` file in the project root with the following variables:
        ```env
        # Blockchain Private Keys
        ETHEREUM_PRIVATE_KEY=your_ethereum_private_key
        POLYGON_PRIVATE_KEY=your_polygon_private_key

        # AI Service
        AI_SERVICE_API_KEY=your_ai_service_api_key
        ```

    - **Note:** Never commit `.env` files to version control. Ensure `.gitignore` includes `.env`.

## Smart Contract Deployment

1. **Prepare Contracts**
    - Place your Solidity contract files in the `contracts/` directory.

2. **Deploy Contracts**
    ```bash
    python deploy_contracts.py
    ```
    - This script will compile and deploy contracts to the specified networks.
    - Deployment details will be saved in `build/deployments_<network>.json`.

## Running the Modules

Each module operates independently but communicates via Kafka for data streaming.

### Data Cleaning

1. **Configure Data Cleaner**
    - Ensure `config.yaml` has the correct settings under `data_cleaner`.

2. **Run Data Cleaner**
    ```bash
    python data_cleaner.py
    ```
    - This script will preprocess and normalize incoming data.

### Twitter Scanning

1. **Configure Twitter Scanning**
    - Update Twitter API credentials in `config.yaml` under `twitter_scanning`.

2. **Run Twitter Scanning Module**
    ```bash
    python twitter_scanning.py
    ```

### Wallet Scanning

1. **Configure Wallet Scanning**
    - Specify wallets to scan in `data/wallets_to_scan.json`.

2. **Run Wallet Scanning Module**
    ```bash
    python wallet_scanning.py
    ```

### Web Scanning

1. **Configure Web Scanning**
    - Define websites to scrape in `data/websites_to_scrape.json`.

2. **Run Web Scanning Module**
    ```bash
    python web_scanning.py
    ```

### Telegram Scanning

1. **Configure Telegram Scanning**
    - Update Telegram API credentials and channels in `config.yaml` under `telegram_scanning`.

2. **Run Telegram Scanning Module**
    ```bash
    python telegram_scanning.py
    ```

### Bundle Scanning

1. **Configure Bundle Scanning**
    - Ensure aggregation rules and AI service details are set in `config.yaml` under `bundle_scanning`.

2. **Run Bundle Scanning Module**
    ```bash
    python bundle_scanning.py
    ```

## Monitoring and Logging

- **Logs:** Each module generates its own log file (e.g., `twitter_scanning.log`, `wallet_scanning.log`).
- **Monitoring Tools:** Set up Prometheus and Grafana as specified in `config.yaml` under `devops_infrastructure`.

## Security Considerations

- **Environment Variables:** Securely manage API keys and private keys using environment variables.
- **Access Controls:** Restrict access to Kafka and other critical services.
- **Regular Audits:** Periodically review smart contracts and system logs for anomalies.

## Troubleshooting

- **Module Failures:** Check the respective log files for error messages.
- **Kafka Issues:** Ensure Kafka and Zookeeper are running. Verify topic configurations.
- **Smart Contract Deployment:** Confirm RPC URLs and private keys are correct. Check `deploy_contracts.log` for details.

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the Repository**
2. **Create a Feature Branch**
    ```bash
    git checkout -b feature/your-feature-name
    ```
3. **Commit Your Changes**
    ```bash
    git commit -m "Add your commit message"
    ```
4. **Push to the Branch**
    ```bash
    git push origin feature/your-feature-name
    ```
5. **Open a Pull Request**

## License

This project is licensed under the [MIT License](LICENSE).

---
*For further assistance, please refer to the project documentation or contact the development team.*

