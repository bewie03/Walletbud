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

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    tx_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    tx_hash TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, tx_hash)
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
            
            # Then add wallet
            await conn.execute(
                '''
                INSERT INTO wallets (user_id, address)
                VALUES ($1, $2)
                ON CONFLICT (user_id, address) DO NOTHING
                ''',
                user_id, address
            )
            return True
            
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
            return 'DELETE' in result
            
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

async def get_all_wallets(user_id: str) -> list[str]:
    """Get all wallets for a user
    
    Args:
        user_id (str): Discord user ID
        
    Returns:
        list[str]: List of wallet addresses
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT address FROM wallets WHERE user_id = $1',
                user_id
            )
            return [row['address'] for row in rows]
            
    except Exception as e:
        logger.error(f"Error getting wallets for user {user_id}: {str(e)}")
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

async def main():
    # Example usage
    try:
        await init_db()
        await add_wallet("1234567890", "addr1...")
        print(await get_all_wallets("1234567890"))
        await remove_wallet("1234567890", "addr1...")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
