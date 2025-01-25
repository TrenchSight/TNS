# twitter_scan_patterns.py

import re
import logging
from typing import Dict, Any, List, Optional

from scanning_patterns import ScanningPatterns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("twitter_scan_patterns.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TwitterScanPatterns:
    """
    Handles pattern matching and detection for Twitter data based on predefined patterns.
    Utilizes regex and keyword matching to identify relevant tokens and potential threats.
    """

    def __init__(self, patterns_path: str = 'config/patterns.json'):
        """
        Initializes the TwitterScanPatterns with loaded patterns.

        Parameters:
        - patterns_path (str): Path to the patterns JSON file.
        """
        self.scanning_patterns = ScanningPatterns(patterns_path)
        if not self.scanning_patterns.validate_patterns():
            logger.error("Invalid scanning patterns configuration.")
            raise ValueError("Invalid scanning patterns configuration.")
        self.twitter_patterns = self.scanning_patterns.get_patterns('twitter_scanning')
        logger.info("TwitterScanPatterns initialized with %d keywords and %d regex patterns.",
                    len(self.twitter_patterns.get('keywords', [])),
                    len(self.twitter_patterns.get('regex_patterns', [])))

    def match_keywords(self, tweet_text: str) -> List[str]:
        """
        Matches keywords in the tweet text.

        Parameters:
        - tweet_text (str): The text content of the tweet.

        Returns:
        - List[str]: List of matched keywords.
        """
        matched_keywords = []
        keywords = self.twitter_patterns.get('keywords', [])
        for keyword in keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', tweet_text, re.IGNORECASE):
                matched_keywords.append(keyword)
        if matched_keywords:
            logger.debug("Matched keywords: %s", matched_keywords)
        return matched_keywords

    def match_regex_patterns(self, tweet_text: str) -> List[Dict[str, Any]]:
        """
        Matches regex patterns in the tweet text and extracts relevant data.

        Parameters:
        - tweet_text (str): The text content of the tweet.

        Returns:
        - List[Dict[str, Any]]: List of matched patterns with extracted groups.
        """
        matched_patterns = []
        regex_patterns = self.twitter_patterns.get('regex_patterns', [])
        for pattern in regex_patterns:
            matches = re.findall(pattern, tweet_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match_dict = {f"group_{i+1}": grp for i, grp in enumerate(match)}
                else:
                    match_dict = {"match": match}
                matched_patterns.append({
                    "pattern": pattern,
                    "match": match_dict
                })
        if matched_patterns:
            logger.debug("Matched regex patterns: %s", matched_patterns)
        return matched_patterns

    def analyze_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes a single tweet for keyword and pattern matches.

        Parameters:
        - tweet (Dict[str, Any]): The tweet data containing at least 'text' and 'id'.

        Returns:
        - Dict[str, Any]: Analysis results including matched keywords and patterns.
        """
        tweet_text = tweet.get('text', '')
        tweet_id = tweet.get('id', '')

        logger.info("Analyzing tweet ID: %s", tweet_id)

        results = {
            'tweet_id': tweet_id,
            'matched_keywords': self.match_keywords(tweet_text),
            'matched_patterns': self.match_regex_patterns(tweet_text),
            'analysis_timestamp': tweet.get('timestamp', datetime.utcnow().isoformat())
        }

        if results['matched_keywords'] or results['matched_patterns']:
            logger.info("Tweet ID %s matched with keywords: %s and patterns: %s",
                        tweet_id, results['matched_keywords'], results['matched_patterns'])
        else:
            logger.info("Tweet ID %s did not match any keywords or patterns.", tweet_id)

        return results

    def process_tweet(self, tweet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Processes a tweet and determines if it warrants further action.

        Parameters:
        - tweet (Dict[str, Any]): The tweet data.

        Returns:
        - Optional[Dict[str, Any]]: Processed analysis if matches are found, else None.
        """
        analysis = self.analyze_tweet(tweet)
        if analysis['matched_keywords'] or analysis['matched_patterns']:
            logger.debug("Tweet ID %s requires further action based on analysis.", analysis['tweet_id'])
            return analysis
        return None

def main():
    """
    Example usage of TwitterScanPatterns.
    """
    import sys
    import json
    from datetime import datetime

    # Initialize TwitterScanPatterns
    try:
        twitter_patterns = TwitterScanPatterns()
    except Exception as e:
        logger.error("Failed to initialize TwitterScanPatterns: %s", e)
        sys.exit(1)

    # Example tweet data
    example_tweets = [
        {
            "id": "1234567890",
            "text": "Launching our new #DeFi token! Visit https://t.me/defi_updates for more info. Total Supply: 1000000",
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "id": "0987654321",
            "text": "Beware of rug pulls! Always check liquidity pool and contract audits before investing.",
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "id": "1122334455",
            "text": "Join our presale now! Limited spots available. https://t.me/presale_channel",
            "timestamp": datetime.utcnow().isoformat()
        }
    ]

    # Process each example tweet
    for tweet in example_tweets:
        analysis = twitter_patterns.process_tweet(tweet)
        if analysis:
            print(json.dumps(analysis, indent=4))
        else:
            print(f"Tweet ID {tweet['id']} does not match any patterns.")

if __name__ == "__main__":
    main()

