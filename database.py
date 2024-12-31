import os
import logging
import asyncpg
from datetime import datetime
import json

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
-- Drop existing tables if they exist
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS users;

-- Users table for storing Discord users
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notification_settings JSONB DEFAULT '{"ada_transactions": true}'
);

-- Wallets table for storing monitored wallets
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id SERIAL PRIMARY KEY,
    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_yummi_check TIMESTAMP,
    last_balance BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, address)
);

-- Transactions table for tracking wallet transactions
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
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(CREATE_TABLES_SQL)
            logger.info("Database initialized successfully")
        return pool
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

async def add_wallet(user_id: str, address: str) -> bool:
    """Add a wallet to monitor
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address to monitor
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # First ensure user exists
            await conn.execute(
                """
                INSERT INTO users (user_id)
                VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_id
            )
            
            # Then add wallet
            await conn.execute(
                """
                INSERT INTO wallets (user_id, address)
                VALUES ($1, $2)
                """,
                user_id, address
            )
            return True
    except Exception as e:
        logger.error(f"Error adding wallet: {str(e)}")
        return False

async def remove_wallet(user_id: str, address: str) -> bool:
    """Remove a wallet from monitoring
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address to remove
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            return True
    except Exception as e:
        logger.error(f"Error removing wallet: {str(e)}")
        return False

async def get_wallet(user_id: str, address: str):
    """Get a specific wallet
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        Record: Wallet record or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchrow(
                """
                SELECT * FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
    except Exception as e:
        logger.error(f"Error getting wallet: {str(e)}")
        return None

async def get_all_wallets():
    """Get all monitored wallets
    
    Returns:
        List[Record]: List of wallet records
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM wallets")
    except Exception as e:
        logger.error(f"Error getting all wallets: {str(e)}")
        return []

async def get_all_wallets_for_user(user_id: str):
    """Get all wallets for a specific user
    
    Args:
        user_id (str): Discord user ID
        
    Returns:
        List[str]: List of wallet addresses
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT address FROM wallets WHERE user_id = $1",
                user_id
            )
            return [row['address'] for row in rows]
    except Exception as e:
        logger.error(f"Error getting user wallets: {str(e)}")
        return []

async def get_user_id_for_wallet(address: str):
    """Get the user ID associated with a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        str: Discord user ID or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT user_id FROM wallets WHERE address = $1",
                address
            )
    except Exception as e:
        logger.error(f"Error getting user ID: {str(e)}")
        return None

async def get_last_yummi_check(address: str):
    """Get the last time YUMMI requirement was checked
    
    Args:
        address (str): Wallet address
        
    Returns:
        datetime: Last check time or None if never checked
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT last_yummi_check FROM wallets WHERE address = $1",
                address
            )
    except Exception as e:
        logger.error(f"Error getting last YUMMI check: {str(e)}")
        return None

async def update_last_yummi_check(address: str):
    """Update the last YUMMI check time
    
    Args:
        address (str): Wallet address
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE wallets 
                SET last_yummi_check = NOW()
                WHERE address = $1
                """,
                address
            )
    except Exception as e:
        logger.error(f"Error updating last YUMMI check: {str(e)}")

async def update_last_checked(wallet_id: int):
    """Update the last checked timestamp
    
    Args:
        wallet_id (int): Wallet ID to update
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE wallets
                SET last_checked = NOW()
                WHERE wallet_id = $1
                """,
                wallet_id
            )
    except Exception as e:
        logger.error(f"Error updating last checked: {str(e)}")

async def get_wallet_id(user_id: str, address: str):
    """Get wallet ID for a user's wallet
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        int: Wallet ID or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT wallet_id FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
    except Exception as e:
        logger.error(f"Error getting wallet ID: {str(e)}")
        return None

async def add_transaction(wallet_id: int, tx_hash: str, timestamp: datetime = None):
    """Add a transaction to the database
    
    Args:
        wallet_id (int): Wallet ID
        tx_hash (str): Transaction hash
        timestamp (datetime, optional): Transaction timestamp. Defaults to current time.
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO transactions (wallet_id, tx_hash, timestamp)
                VALUES ($1, $2, $3)
                ON CONFLICT (wallet_id, tx_hash) DO NOTHING
                """,
                wallet_id, tx_hash, timestamp or datetime.utcnow()
            )
            return True
    except Exception as e:
        logger.error(f"Error adding transaction: {str(e)}")
        return False

async def get_notification_settings(user_id: str):
    """Get user's notification settings
    
    Args:
        user_id (str): Discord user ID
        
    Returns:
        dict: Dictionary of notification settings
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # First ensure user exists with default settings
            await conn.execute(
                """
                INSERT INTO users (user_id, notification_settings)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_id,
                json.dumps({
                    "ada_transactions": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                })
            )
            
            # Get settings
            row = await conn.fetchrow(
                "SELECT notification_settings FROM users WHERE user_id = $1",
                user_id
            )
            return json.loads(row['notification_settings']) if row else None
    except Exception as e:
        logger.error(f"Error getting notification settings: {str(e)}")
        return None

async def update_notification_setting(user_id: str, setting: str, enabled: bool):
    """Update a specific notification setting
    
    Args:
        user_id (str): Discord user ID
        setting (str): Setting name
        enabled (bool): Whether to enable or disable
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # First ensure user exists with default settings
            await conn.execute(
                """
                INSERT INTO users (user_id, notification_settings)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_id,
                json.dumps({
                    "ada_transactions": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                })
            )
            
            # Update specific setting
            await conn.execute(
                """
                UPDATE users 
                SET notification_settings = jsonb_set(
                    COALESCE(notification_settings, '{}'::jsonb),
                    ARRAY[$2],
                    $3::jsonb,
                    true
                )
                WHERE user_id = $1
                """,
                user_id, 
                setting,
                json.dumps(enabled)
            )
            return True
    except Exception as e:
        logger.error(f"Error updating notification setting: {str(e)}")
        return False

async def should_notify(user_id: str, notification_type: str) -> bool:
    """Check if a user should be notified about a specific event
    
    Args:
        user_id (str): Discord user ID
        notification_type (str): Type of notification to check
        
    Returns:
        bool: Whether user should be notified
    """
    try:
        settings = await get_notification_settings(user_id)
        if not settings:
            return True  # Default to notify if no settings
        return settings.get(notification_type, True)
    except Exception as e:
        logger.error(f"Error checking notification settings: {str(e)}")
        return True  # Default to notify on error

async def get_recent_transactions(address: str, hours: int = 1) -> list:
    """Get transactions in the last N hours
    
    Args:
        address (str): The wallet address
        hours (int): Number of hours to look back
        
    Returns:
        list: List of transactions
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT t.* FROM transactions t
                JOIN wallets w ON t.wallet_id = w.wallet_id
                WHERE w.address = $1
                AND t.timestamp > NOW() - interval '$2 hours'
                ORDER BY t.timestamp DESC
                """,
                address, hours
            )
    except Exception as e:
        logger.error(f"Error getting recent transactions: {str(e)}")
        return []

async def check_ada_balance(address: str) -> tuple[bool, int]:
    """Check if ADA balance is below threshold
    
    Args:
        address (str): The wallet address
        
    Returns:
        tuple[bool, int]: (is_below_threshold, current_balance_ada)
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            balance = await conn.fetchval(
                "SELECT last_balance FROM wallets WHERE address = $1",
                address
            )
            
            if balance is None:
                return False, 0
                
            balance_ada = balance / 1_000_000
            return balance_ada < 10, balance_ada
    except Exception as e:
        logger.error(f"Error checking ADA balance: {str(e)}")
        return False, 0

async def update_wallet_balance(address: str, balance: int):
    """Update wallet's ADA balance
    
    Args:
        address (str): Wallet address
        balance (int): Current ADA balance in lovelace
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE wallets 
                SET last_balance = $1,
                    last_checked = NOW()
                WHERE address = $2
                """,
                balance,
                address
            )
    except Exception as e:
        logger.error(f"Error updating wallet balance: {str(e)}")

async def get_wallet_balance(address: str) -> int:
    """Get wallet's current ADA balance
    
    Args:
        address (str): Wallet address
        
    Returns:
        int: Current ADA balance in lovelace
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            balance = await conn.fetchval(
                "SELECT last_balance FROM wallets WHERE address = $1",
                address
            )
            return balance if balance is not None else 0
    except Exception as e:
        logger.error(f"Error getting wallet balance: {str(e)}")
        return 0

async def main():
    """Example usage of database functions"""
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
