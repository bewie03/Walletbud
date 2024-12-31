import os
import logging
import asyncpg
from typing import Tuple, Set, Optional

logger = logging.getLogger(__name__)

# Database connection
async def get_db_pool():
    """Get or create database connection pool"""
    if not hasattr(get_db_pool, '_pool'):
        try:
            DATABASE_URL = os.getenv('DATABASE_URL')
            if not DATABASE_URL:
                raise ValueError("DATABASE_URL environment variable not set")
            
            # Handle special Heroku postgres:// URLs
            if DATABASE_URL.startswith("postgres://"):
                DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            
            get_db_pool._pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Database pool created successfully")
        except Exception as e:
            logger.error(f"Error creating database pool: {str(e)}")
            raise
    return get_db_pool._pool

async def init_db():
    """Initialize database tables"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Create tables if they don't exist
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS monitored_wallets (
                    address TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    stake_address TEXT,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id BIGINT PRIMARY KEY,
                    ada_transactions BOOLEAN DEFAULT TRUE,
                    token_changes BOOLEAN DEFAULT TRUE,
                    nft_updates BOOLEAN DEFAULT TRUE,
                    stake_changes BOOLEAN DEFAULT TRUE,
                    policy_expiry BOOLEAN DEFAULT TRUE,
                    delegation_status BOOLEAN DEFAULT TRUE,
                    staking_rewards BOOLEAN DEFAULT TRUE,
                    dapp_interactions BOOLEAN DEFAULT TRUE,
                    failed_transactions BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS stake_pools (
                    stake_address TEXT PRIMARY KEY,
                    pool_id TEXT,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

async def get_user_id_for_wallet(address: str) -> Optional[int]:
    """Get user ID associated with a wallet address"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT user_id FROM monitored_wallets WHERE address = $1',
                address
            )
            return row['user_id'] if row else None
    except Exception as e:
        logger.error(f"Error getting user ID for wallet {address}: {str(e)}")
        return None

async def get_addresses_for_stake(stake_address: str) -> Set[str]:
    """Get all addresses associated with a stake address"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT address FROM monitored_wallets WHERE stake_address = $1',
                stake_address
            )
            return {row['address'] for row in rows}
    except Exception as e:
        logger.error(f"Error getting addresses for stake {stake_address}: {str(e)}")
        return set()

async def update_pool_for_stake(stake_address: str, pool_id: Optional[str]) -> bool:
    """Update or remove pool ID for a stake address"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            if pool_id:
                await conn.execute('''
                    INSERT INTO stake_pools (stake_address, pool_id, updated_at)
                    VALUES ($1, $2, CURRENT_TIMESTAMP)
                    ON CONFLICT (stake_address) 
                    DO UPDATE SET 
                        pool_id = $2,
                        updated_at = CURRENT_TIMESTAMP
                ''', stake_address, pool_id)
            else:
                await conn.execute(
                    'DELETE FROM stake_pools WHERE stake_address = $1',
                    stake_address
                )
            return True
    except Exception as e:
        logger.error(f"Error updating pool for stake {stake_address}: {str(e)}")
        return False

async def get_all_monitored_addresses() -> Tuple[Set[str], Set[str]]:
    """Get all monitored addresses and stake addresses"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT address, stake_address FROM monitored_wallets')
            addresses = {row['address'] for row in rows}
            stake_addresses = {row['stake_address'] for row in rows if row['stake_address']}
            return addresses, stake_addresses
    except Exception as e:
        logger.error(f"Error getting monitored addresses: {str(e)}")
        return set(), set()

async def should_notify(user_id: int, notification_type: str) -> bool:
    """Check if a user should receive a specific type of notification"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT {notification_type} FROM user_settings WHERE user_id = $1',
                user_id
            )
            return row[notification_type] if row else True  # Default to True if no settings
    except Exception as e:
        logger.error(f"Error checking notification settings for user {user_id}: {str(e)}")
        return True  # Default to True on error to avoid missing important notifications
