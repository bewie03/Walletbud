import os
import logging
import asyncpg
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Global connection pool
_pool = None

async def get_pool():
    """Get or create database connection pool"""
    global _pool
    if _pool is None:
        try:
            _pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Created database connection pool")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            raise
    return _pool

# Create tables SQL
CREATE_TABLES_SQL = """
-- Drop existing tables
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS wallet_states;
DROP TABLE IF EXISTS processed_rewards;
DROP TABLE IF EXISTS stake_addresses;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Wallets table
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id SERIAL PRIMARY KEY,
    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, address)
);

-- Wallet states table
CREATE TABLE IF NOT EXISTS wallet_states (
    address TEXT PRIMARY KEY,
    utxo_state TEXT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    tx_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    tx_hash TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, tx_hash)
);

-- Processed rewards table
CREATE TABLE IF NOT EXISTS processed_rewards (
    address TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    PRIMARY KEY (address, epoch)
);

-- Stake addresses table
CREATE TABLE IF NOT EXISTS stake_addresses (
    address TEXT PRIMARY KEY,
    stake_address TEXT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

async def init_db():
    """Initialize database and create tables"""
    try:
        # Get pool
        pool = await get_pool()
        
        # Create tables
        async with pool.acquire() as conn:
            await conn.execute(CREATE_TABLES_SQL)
            
        logger.info("Database initialized successfully")
        return pool
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

async def add_wallet(user_id: str, address: str) -> bool:
    """Add a wallet to monitor"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # First ensure user exists
            await conn.execute(
                'INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING',
                user_id
            )
            
            # Then add wallet and check if it was actually added
            result = await conn.execute(
                '''
                INSERT INTO wallets (user_id, address)
                VALUES ($1, $2)
                ON CONFLICT (user_id, address) DO NOTHING
                ''',
                user_id, address
            )
            # Check if a row was actually inserted
            return result.split()[2] == '1'
            
    except Exception as e:
        logger.error(f"Error adding wallet: {str(e)}")
        return False

async def remove_wallet(user_id: str, address: str) -> bool:
    """Remove a wallet from monitoring"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM wallets WHERE user_id = $1 AND address = $2',
                user_id, address
            )
            # Check if any rows were actually deleted
            return result.split()[1] != '0'
            
    except Exception as e:
        logger.error(f"Error removing wallet: {str(e)}")
        return False

async def get_wallet(user_id: str, address: str):
    """Get a specific wallet"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchrow(
                'SELECT * FROM wallets WHERE user_id = $1 AND address = $2',
                user_id, address
            )
            
    except Exception as e:
        logger.error(f"Error getting wallet: {str(e)}")
        return None

async def get_all_wallets():
    """Get all wallets from the database
    
    Returns:
        list: List of wallet dictionaries with user_id and address
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as connection:
            return await connection.fetch("SELECT * FROM wallets")
    except Exception as e:
        logger.error(f"Error getting all wallets: {str(e)}")
        return []

async def update_last_checked(wallet_id: int, timestamp: datetime = None):
    """Update last checked timestamp for a wallet"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'UPDATE wallets SET last_checked = $1 WHERE wallet_id = $2',
                timestamp or datetime.utcnow(), wallet_id
            )
            return True
            
    except Exception as e:
        logger.error(f"Error updating last checked: {str(e)}")
        return False

async def add_transaction(wallet_id: int, tx_hash: str, timestamp: datetime = None):
    """Add a transaction"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO transactions (wallet_id, tx_hash, timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT (wallet_id, tx_hash) DO NOTHING
                ''',
                wallet_id, tx_hash, timestamp or datetime.utcnow()
            )
            return True
            
    except Exception as e:
        logger.error(f"Error adding transaction: {str(e)}")
        return False

async def get_user_id_for_wallet(address: str) -> str:
    """Get the user ID associated with a wallet address
    
    Args:
        address (str): The wallet address to look up
        
    Returns:
        str: The user ID if found, None otherwise
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                'SELECT user_id FROM wallets WHERE address = $1',
                address
            )
            return result
            
    except Exception as e:
        logger.error(f"Error getting user ID for wallet: {str(e)}")
        return None

async def store_utxo_state(address: str, utxo_state: str) -> bool:
    """Store the current UTXO state for a wallet
    
    Args:
        address (str): The wallet address
        utxo_state (str): JSON string of current UTXOs
        
    Returns:
        bool: True if successful
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO wallet_states (address, utxo_state)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET utxo_state = $2, last_updated = NOW()
                ''',
                address, utxo_state
            )
            return True
            
    except Exception as e:
        logger.error(f"Error storing UTXO state: {str(e)}")
        return False

async def get_utxo_state(address: str) -> str:
    """Get the last stored UTXO state for a wallet
    
    Args:
        address (str): The wallet address
        
    Returns:
        str: JSON string of stored UTXOs, or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT utxo_state FROM wallet_states WHERE address = $1',
                address
            )
            
    except Exception as e:
        logger.error(f"Error getting UTXO state: {str(e)}")
        return None

async def is_reward_processed(address: str, epoch: int) -> bool:
    """Check if a staking reward has been processed
    
    Args:
        address (str): The wallet address
        epoch (int): The epoch number
        
    Returns:
        bool: True if reward was already processed
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM processed_rewards WHERE address = $1 AND epoch = $2)',
                address, epoch
            )
            return result
            
    except Exception as e:
        logger.error(f"Error checking processed reward: {str(e)}")
        return False

async def add_processed_reward(address: str, epoch: int) -> bool:
    """Mark a staking reward as processed
    
    Args:
        address (str): The wallet address
        epoch (int): The epoch number
        
    Returns:
        bool: True if successful
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO processed_rewards (address, epoch) VALUES ($1, $2)',
                address, epoch
            )
            return True
            
    except Exception as e:
        logger.error(f"Error adding processed reward: {str(e)}")
        return False

async def get_stake_address(address: str) -> str:
    """Get the stored stake address for a wallet
    
    Args:
        address (str): The wallet address
        
    Returns:
        str: The stake address if found, None otherwise
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT stake_address FROM stake_addresses WHERE address = $1',
                address
            )
            
    except Exception as e:
        logger.error(f"Error getting stake address: {str(e)}")
        return None

async def update_stake_address(address: str, stake_address: str) -> bool:
    """Update the stake address for a wallet
    
    Args:
        address (str): The wallet address
        stake_address (str): The new stake address
        
    Returns:
        bool: True if successful
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO stake_addresses (address, stake_address)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET stake_address = $2, last_updated = NOW()
                ''',
                address, stake_address
            )
            return True
            
    except Exception as e:
        logger.error(f"Error updating stake address: {str(e)}")
        return False

async def main():
    # Example usage
    try:
        await init_db()
        await add_wallet("1234567890", "addr1...")
        print(await get_all_wallets())
        await remove_wallet("1234567890", "addr1...")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
