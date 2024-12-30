import os
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import time
from sqlalchemy.exc import OperationalError
import logging

logger = logging.getLogger(__name__)

# Get database URL from environment variable (Heroku provides this)
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    logger.error("No DATABASE_URL found in environment variables!")
    DATABASE_URL = 'sqlite:///walletbud.db'
    logger.info("Falling back to SQLite database")

# Convert postgres:// to postgresql:// for SQLAlchemy 1.4+
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    logger.info("Converted postgres:// to postgresql:// in DATABASE_URL")

try:
    # Create SQLAlchemy engine and base
    engine = create_engine(DATABASE_URL)
    # Test the connection
    with engine.connect() as conn:
        logger.info("Database connection successful")
except Exception as e:
    logger.error(f"Failed to connect to database: {str(e)}")
    raise

Base = declarative_base()
Session = sessionmaker(bind=engine)

class User(Base):
    __tablename__ = 'users'
    discord_id = Column(String, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    wallets = relationship("Wallet", backref="user")

class Wallet(Base):
    __tablename__ = 'wallets'
    wallet_address = Column(String, primary_key=True)
    discord_id = Column(String, ForeignKey('users.discord_id'))
    last_checked = Column(DateTime)
    is_active = Column(Boolean, default=False)

class Database:
    def __init__(self):
        Base.metadata.create_all(engine)
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = Session()
        return self._session

    def close(self):
        """Close the database session"""
        if self._session:
            self._session.close()
            self._session = None

    def __del__(self):
        """Ensure session is closed when object is destroyed"""
        self.close()

    def add_user(self, discord_id):
        """Add a new user to the database"""
        try:
            user = User(discord_id=discord_id)
            self.session.merge(user)
            self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding user: {str(e)}")
            self.session.rollback()
            return False

    def add_wallet(self, discord_id, wallet_address):
        """Add a new wallet for a user"""
        try:
            wallet = Wallet(
                wallet_address=wallet_address,
                discord_id=discord_id,
                last_checked=datetime.utcnow()
            )
            self.session.merge(wallet)
            self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            self.session.rollback()
            return False

    def get_user_wallets(self, discord_id):
        """Get all wallets for a specific user"""
        wallets = self.session.query(Wallet).filter_by(discord_id=discord_id).all()
        return [wallet.wallet_address for wallet in wallets]

    def update_wallet_status(self, wallet_address, is_active):
        """Update wallet active status based on token holdings"""
        try:
            wallet = self.session.query(Wallet).filter_by(wallet_address=wallet_address).first()
            if wallet:
                wallet.is_active = is_active
                wallet.last_checked = datetime.utcnow()
                self.session.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Error updating wallet status: {str(e)}")
            self.session.rollback()
            return False

    def get_all_active_wallets(self):
        """Get all active wallets for monitoring"""
        wallets = self.session.query(Wallet).filter_by(is_active=True).all()
        return [(w.wallet_address, w.discord_id) for w in wallets]

    def remove_wallet(self, discord_id, wallet_address):
        """Remove a wallet from tracking"""
        try:
            wallet = self.session.query(Wallet).filter_by(
                wallet_address=wallet_address,
                discord_id=discord_id
            ).first()
            if wallet:
                self.session.delete(wallet)
                self.session.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            self.session.rollback()
            return False

    def update_last_checked(self, wallet_address):
        """Update the last_checked timestamp for a wallet"""
        try:
            wallet = self.session.query(Wallet).filter_by(wallet_address=wallet_address).first()
            if wallet:
                wallet.last_checked = datetime.utcnow()
                self.session.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Error updating last checked time: {str(e)}")
            self.session.rollback()
            return False

    def get_last_checked(self, wallet_address):
        """Get the last checked time for a wallet"""
        try:
            wallet = self.session.query(Wallet).filter_by(wallet_address=wallet_address).first()
            return wallet.last_checked if wallet else None
        except Exception as e:
            logger.error(f"Error getting last checked time: {str(e)}")
            return None
