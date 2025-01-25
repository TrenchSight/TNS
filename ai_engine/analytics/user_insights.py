# user_insights.py

import os
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, ForeignKey, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session, relationship
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("user_insights.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Load configuration
CONFIG_PATH = os.getenv('CONFIG_PATH', 'config.yaml')

def load_configuration(config_path: str) -> Dict[str, Any]:
    """
    Loads the configuration from a YAML file.

    Parameters:
    - config_path (str): Path to the configuration file.

    Returns:
    - dict: Configuration dictionary.
    """
    if not os.path.exists(config_path):
        logger.error(f"Configuration file {config_path} does not exist.")
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}.")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse configuration file {config_path}: {e}")
        raise

config = load_configuration(CONFIG_PATH)

# Database setup
DATABASE_URL = config.get('database', {}).get('url')
if not DATABASE_URL:
    logger.error("Database URL not provided in configuration.")
    raise ValueError("Database URL must be provided in configuration.")

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ORM Models
class BlockchainAnalysis(Base):
    __tablename__ = 'blockchain_analysis'
    id = Column(Integer, primary_key=True, index=True)
    network = Column(String(50), nullable=False)
    transaction_hash = Column(String(66), unique=True, nullable=False)
    from_address = Column(String(42), nullable=False)
    to_address = Column(String(42), nullable=True)
    value = Column(Float, nullable=False)
    gas = Column(Integer, nullable=False)
    gas_price = Column(Float, nullable=False)
    nonce = Column(Integer, nullable=False)
    block_number = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    patterns_matched = Column(String(255), nullable=True)
    analysis_timestamp = Column(DateTime, default=datetime.utcnow)

class PumpFunAnalysis(Base):
    __tablename__ = 'pumpfun_analysis'
    id = Column(Integer, primary_key=True, index=True)
    network = Column(String(50), nullable=False, default='pumpfun')
    project_name = Column(String(255), nullable=False)
    price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    launch_date = Column(String(50), nullable=True)
    contract_address = Column(String(42), nullable=True)
    liquidity_locked = Column(String(10), nullable=True)
    audit_status = Column(String(20), nullable=True)
    patterns_matched = Column(String(255), nullable=True)
    analysis_timestamp = Column(DateTime, default=datetime.utcnow)

# Add other ORM models as needed...

# Pydantic Schemas
class BlockchainAnalysisSchema(BaseModel):
    id: int
    network: str
    transaction_hash: str
    from_address: str
    to_address: Optional[str]
    value: float
    gas: int
    gas_price: float
    nonce: int
    block_number: int
    timestamp: datetime
    patterns_matched: Optional[str]
    analysis_timestamp: datetime

    class Config:
        orm_mode = True

class PumpFunAnalysisSchema(BaseModel):
    id: int
    network: str
    project_name: str
    price: float
    volume: float
    launch_date: Optional[str]
    contract_address: Optional[str]
    liquidity_locked: Optional[str]
    audit_status: Optional[str]
    patterns_matched: Optional[str]
    analysis_timestamp: datetime

    class Config:
        orm_mode = True

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize FastAPI
app = FastAPI(
    title="TrenchSight User Insights API",
    description="API for accessing user insights, analytics, and governance data in TrenchSight platform.",
    version="1.0.0"
)

# Routes
@app.get("/main_account", response_model=Dict[str, Any])
def get_main_account_insights(db: Session = Depends(get_db)):
    """
    Retrieves high-level analytics, trending tokens, and governance proposals.
    """
    try:
        # Example: Get top 10 tokens by volume from PumpFunAnalysis
        top_tokens = db.query(
            PumpFunAnalysis.project_name,
            func.sum(PumpFunAnalysis.volume).label('total_volume')
        ).group_by(PumpFunAnalysis.project_name).order_by(func.sum(PumpFunAnalysis.volume).desc()).limit(10).all()

        top_tokens_list = [{"project_name": token.project_name, "total_volume": token.total_volume} for token in top_tokens]

        # Example: Get recent governance proposals
        # Placeholder: Assuming there's a GovernanceProposal model
        # governance_proposals = db.query(GovernanceProposal).order_by(GovernanceProposal.created_at.desc()).limit(5).all()
        # governance_proposals_list = [proposal.to_dict() for proposal in governance_proposals]

        governance_proposals_list = []  # Replace with actual data retrieval

        insights = {
            "trending_tokens": top_tokens_list,
            "governance_proposals": governance_proposals_list,
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info("Retrieved main account insights.")
        return insights

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

@app.get("/agents", response_model=List[Dict[str, Any]])
def get_agents_signals(db: Session = Depends(get_db)):
    """
    Retrieves live feed of signals generated by individual Swarm Agents.
    """
    try:
        # Example: Fetch latest 50 blockchain analysis records
        latest_blockchain = db.query(BlockchainAnalysis).order_by(BlockchainAnalysis.analysis_timestamp.desc()).limit(50).all()
        blockchain_signals = [record.__dict__ for record in latest_blockchain]

        # Example: Fetch latest 50 PumpFunAnalysis records
        latest_pumpfun = db.query(PumpFunAnalysis).order_by(PumpFunAnalysis.analysis_timestamp.desc()).limit(50).all()
        pumpfun_signals = [record.__dict__ for record in latest_pumpfun]

        # Combine signals
        combined_signals = blockchain_signals + pumpfun_signals
        # Sort combined signals by timestamp descending
        combined_signals_sorted = sorted(combined_signals, key=lambda x: x['analysis_timestamp'], reverse=True)

        logger.info("Retrieved agents' signals.")
        return combined_signals_sorted

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

@app.post("/governance/proposals", response_model=Dict[str, Any])
def submit_governance_proposal(proposal: Dict[str, Any], db: Session = Depends(get_db)):
    """
    Submits a new governance proposal.
    """
    try:
        # Placeholder: Assuming there's a GovernanceProposal model
        # new_proposal = GovernanceProposal(**proposal)
        # db.add(new_proposal)
        # db.commit()
        # db.refresh(new_proposal)
        # return new_proposal

        # Since GovernanceProposal model is not defined, return a success message
        logger.info("Received new governance proposal submission.")
        return {"message": "Governance proposal submitted successfully."}

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred while submitting proposal: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

@app.post("/governance/vote", response_model=Dict[str, Any])
def vote_on_proposal(vote: Dict[str, Any], db: Session = Depends(get_db)):
    """
    Casts a vote on a governance proposal.
    """
    try:
        # Placeholder: Assuming there's a GovernanceVote model
        # new_vote = GovernanceVote(**vote)
        # db.add(new_vote)
        # db.commit()
        # return new_vote

        # Since GovernanceVote model is not defined, return a success message
        logger.info("Received vote on governance proposal.")
        return {"message": "Vote cast successfully."}

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred while casting vote: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

# Additional endpoints can be added here for more functionalities

# Run the app using Uvicorn
# To run the app, execute the following command in the terminal:
# uvicorn user_insights:app --reload


