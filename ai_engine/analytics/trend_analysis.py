# trend_analysis.py

import os
import json
import logging
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import yaml
from dotenv import load_dotenv

from scanning_patterns import ScanningPatterns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("trend_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TrendAnalysisConfig:
    """
    Configuration loader for Trend Analysis module.
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
        required_sections = ['trend_analysis', 'data_split', 'kafka', 'ai_model_features']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")

        trend_config = self.config['trend_analysis']
        required_keys = ['output_dir', 'visualization']
        for key in required_keys:
            if key not in trend_config:
                logger.error(f"Missing required config parameter in trend_analysis: {key}")
                raise KeyError(f"Missing required config parameter in trend_analysis: {key}")

        logger.info("TrendAnalysis configuration validated successfully.")


class TrendAnalyzer:
    """
    Handles trend analysis on preprocessed cryptocurrency data.
    Utilizes statistical methods and visualization to identify market trends.
    """

    def __init__(self, config: TrendAnalysisConfig):
        """
        Initializes the TrendAnalyzer with configuration parameters.

        Parameters:
        - config (TrendAnalysisConfig): The configuration object.
        """
        self.input_path = config.config['data_split'].get('output_dir', 'data/splits')
        self.output_dir = config.config['trend_analysis'].get('output_dir', 'data/trends')
        self.visualization = config.config['trend_analysis'].get('visualization', True)
        self.feature_columns = config.config['ai_model_features'].get('token_launch', [])
        self.visualization_config = config.config['trend_analysis'].get('visualization_config', {})
        logger.info(f"TrendAnalyzer initialized with input_path: {self.input_path}, output_dir: {self.output_dir}, visualization: {self.visualization}")
        os.makedirs(self.output_dir, exist_ok=True)

    def load_data(self) -> pd.DataFrame:
        """
        Loads preprocessed data from CSV files.

        Returns:
        - pd.DataFrame: Combined DataFrame containing training, validation, and test data.
        """
        try:
            train_path = os.path.join(self.input_path, 'train.csv')
            validation_path = os.path.join(self.input_path, 'validation.csv')
            test_path = os.path.join(self.input_path, 'test.csv')

            train_df = pd.read_csv(train_path)
            validation_df = pd.read_csv(validation_path)
            test_df = pd.read_csv(test_path)

            combined_df = pd.concat([train_df, validation_df, test_df], ignore_index=True)
            logger.info(f"Loaded and combined data with shape {combined_df.shape}.")
            return combined_df
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

    def perform_trend_analysis(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Performs trend analysis using linear regression on token prices over time.

        Parameters:
        - data (pd.DataFrame): The combined data for analysis.

        Returns:
        - pd.DataFrame: DataFrame containing trend metrics for each project.
        """
        logger.info("Starting trend analysis.")
        trend_results = []

        # Ensure 'launch_date' is datetime
        if 'launch_date' in data.columns:
            data['launch_date'] = pd.to_datetime(data['launch_date'])
        else:
            logger.error("'launch_date' column not found in data.")
            raise KeyError("'launch_date' column not found in data.")

        projects = data['project_name'].unique()
        logger.info(f"Performing trend analysis on {len(projects)} projects.")

        for project in projects:
            project_data = data[data['project_name'] == project].sort_values('launch_date')
            if project_data.empty:
                continue

            # Feature engineering: Convert dates to ordinal for regression
            project_data['date_ordinal'] = project_data['launch_date'].map(datetime.toordinal)

            X = project_data[['date_ordinal']]
            y = project_data['price']

            # Check if there are enough data points
            if len(project_data) < 2:
                logger.warning(f"Not enough data points for project {project} to perform trend analysis.")
                continue

            # Perform linear regression
            model = LinearRegression()
            model.fit(X, y)
            y_pred = model.predict(X)
            slope = model.coef_[0]
            intercept = model.intercept_
            mse = mean_squared_error(y, y_pred)

            trend = 'uptrend' if slope > 0 else 'downtrend' if slope < 0 else 'no_trend'

            trend_results.append({
                'project_name': project,
                'slope': slope,
                'intercept': intercept,
                'mse': mse,
                'trend': trend
            })

            logger.debug(f"Project: {project}, Slope: {slope}, Trend: {trend}, MSE: {mse}")

        trend_df = pd.DataFrame(trend_results)
        logger.info(f"Completed trend analysis with results shape {trend_df.shape}.")
        return trend_df

    def visualize_trends(self, data: pd.DataFrame, trend_df: pd.DataFrame):
        """
        Generates and saves trend visualizations for each project.

        Parameters:
        - data (pd.DataFrame): The combined data for visualization.
        - trend_df (pd.DataFrame): The trend analysis results.
        """
        logger.info("Starting trend visualization.")
        for _, row in trend_df.iterrows():
            project = row['project_name']
            slope = row['slope']
            trend = row['trend']

            project_data = data[data['project_name'] == project].sort_values('launch_date')
            if project_data.empty:
                continue

            fig = px.scatter(project_data, x='launch_date', y='price', title=f"Price Trend for {project}",
                             labels={'launch_date': 'Launch Date', 'price': 'Price (TNS)'})
            # Add trend line
            fig.add_trace(px.line(project_data, x='launch_date', y='price').data[0])
            fig.add_shape(
                type="line",
                x0=project_data['launch_date'].min(),
                y0=row['intercept'] + slope * project_data['launch_date'].min().toordinal(),
                x1=project_data['launch_date'].max(),
                y1=row['intercept'] + slope * project_data['launch_date'].max().toordinal(),
                line=dict(color="Red" if trend == 'downtrend' else "Green", dash="dash")
            )
            fig.update_layout(showlegend=False)

            # Save visualization
            viz_path = os.path.join(self.output_dir, f"{project}_trend.html")
            fig.write_html(viz_path)
            logger.info(f"Saved trend visualization for {project} at {viz_path}")

    def save_trend_results(self, trend_df: pd.DataFrame):
        """
        Saves the trend analysis results to a CSV file.

        Parameters:
        - trend_df (pd.DataFrame): The trend analysis results.
        """
        try:
            output_path = os.path.join(self.output_dir, 'trend_results.csv')
            trend_df.to_csv(output_path, index=False)
            logger.info(f"Trend analysis results saved to {output_path}.")
        except Exception as e:
            logger.error(f"Failed to save trend analysis results: {e}")
            raise

    def run(self):
        """
        Executes the full trend analysis pipeline.
        """
        try:
            data = self.load_data()
            trend_df = self.perform_trend_analysis(data)
            self.save_trend_results(trend_df)
            if self.visualization:
                self.visualize_trends(data, trend_df)
            logger.info("Trend analysis pipeline executed successfully.")
        except Exception as e:
            logger.error(f"Trend analysis pipeline failed: {e}")


def load_configuration(config_path: str = 'config.yaml') -> TrendAnalysisConfig:
    """
    Loads the trend analysis configuration.

    Parameters:
    - config_path (str): Path to the configuration file.

    Returns:
    - TrendAnalysisConfig: The loaded configuration object.
    """
    config = TrendAnalysisConfig(config_path)
    return config


def main():
    """
    Main function to execute the trend analysis.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load configuration
    try:
        config = load_configuration()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Initialize and run TrendAnalyzer
    try:
        analyzer = TrendAnalyzer(config)
        analyzer.run()
    except Exception as e:
        logger.error(f"TrendAnalyzer encountered an error: {e}")


if __name__ == "__main__":
    main()

