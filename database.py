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
DROP TABLE IF EXISTS rewards;
DROP TABLE IF EXISTS yummi_warnings;
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
    stake_address TEXT,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_yummi_check TIMESTAMP,
    last_balance BIGINT,
    utxo_state JSONB,
    UNIQUE(user_id, address)
);

-- Transactions table for storing transaction history
CREATE TABLE IF NOT EXISTS transactions (
    tx_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    tx_hash TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, tx_hash)
);

-- Rewards table for tracking processed staking rewards
CREATE TABLE IF NOT EXISTS rewards (
    reward_id SERIAL PRIMARY KEY,
    stake_address TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    amount BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stake_address, epoch)
);

-- YUMMI warning table for tracking warning counts
CREATE TABLE IF NOT EXISTS yummi_warnings (
    warning_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    warning_count INTEGER DEFAULT 0,
    last_warning_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id)
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

async def update_ada_balance(address: str, balance: float) -> bool:
    """Update wallet's ADA balance
    
    Args:
        address (str): Wallet address
        balance (float): Current ADA balance
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE wallets 
                SET last_balance = $2 
                WHERE address = $1
                """,
                address, int(balance * 1_000_000)  # Convert to lovelace
            )
            return True
    except Exception as e:
        logger.error(f"Error updating ADA balance: {str(e)}")
        return False

async def update_token_balances(address: str, token_balances: dict) -> bool:
    """Update wallet's token balances
    
    Args:
        address (str): Wallet address
        token_balances (dict): Dictionary of token_id -> amount
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE wallets 
                SET utxo_state = $2 
                WHERE address = $1
                """,
                address, json.dumps(token_balances)
            )
            return True
    except Exception as e:
        logger.error(f"Error updating token balances: {str(e)}")
        return False

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

async def update_utxo_state(address: str, utxo_state: dict):
    """Update the UTxO state for a wallet address
    
    Args:
        address (str): Wallet address
        utxo_state (dict): New UTxO state
        
    Returns:
        bool: Success status
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                UPDATE wallets
                SET utxo_state = $1
                WHERE address = $2
                """,
                utxo_state, address
            )
            return True
        except Exception as e:
            logger.error(f"Error updating UTxO state for {address}: {str(e)}")
            return False

# Alias for backward compatibility
store_utxo_state = update_utxo_state

async def get_stake_address(address: str):
    """Get the stake address for a wallet address
    
    Args:
        address (str): Wallet address
        
    Returns:
        str: Stake address or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT stake_address
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return row['stake_address'] if row else None
        except Exception as e:
            logger.error(f"Error getting stake address for {address}: {str(e)}")
            return None

async def update_stake_address(address: str, stake_address: str):
    """Update the stake address for a wallet
    
    Args:
        address (str): Wallet address
        stake_address (str): Stake address
        
    Returns:
        bool: Success status
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                UPDATE wallets
                SET stake_address = $1
                WHERE address = $2
                """,
                stake_address, address
            )
            return True
        except Exception as e:
            logger.error(f"Error updating stake address for {address}: {str(e)}")
            return False

async def is_reward_processed(stake_address: str, epoch: int):
    """Check if a staking reward has been processed
    
    Args:
        stake_address (str): Stake address
        epoch (int): Epoch number
        
    Returns:
        bool: True if reward was processed
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT reward_id
                FROM rewards
                WHERE stake_address = $1 AND epoch = $2
                """,
                stake_address, epoch
            )
            return bool(row)
        except Exception as e:
            logger.error(f"Error checking reward for {stake_address} epoch {epoch}: {str(e)}")
            return False

async def add_processed_reward(stake_address: str, epoch: int, amount: int):
    """Add a processed staking reward
    
    Args:
        stake_address (str): Stake address
        epoch (int): Epoch number
        amount (int): Reward amount in lovelace
        
    Returns:
        bool: Success status
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO rewards (stake_address, epoch, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (stake_address, epoch) DO NOTHING
                """,
                stake_address, epoch, amount
            )
            return True
        except Exception as e:
            logger.error(f"Error adding reward for {stake_address} epoch {epoch}: {str(e)}")
            return False

async def get_last_transactions(address: str):
    """Retrieve the last set of processed transactions for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        List[str]: List of transaction hashes
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            wallet_id = await conn.fetchval(
                """
                SELECT wallet_id
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            if not wallet_id:
                return []
                
            rows = await conn.fetch(
                """
                SELECT tx_hash
                FROM transactions
                WHERE wallet_id = $1
                ORDER BY timestamp DESC
                LIMIT 10
                """,
                wallet_id
            )
            return [row['tx_hash'] for row in rows]
        except Exception as e:
            logger.error(f"Error getting last transactions for {address}: {str(e)}")
            return []

async def get_utxo_state(address: str):
    """Get the UTxO state for a wallet address
    
    Args:
        address (str): Wallet address
        
    Returns:
        dict: Dictionary containing UTxO state or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT utxo_state
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return row['utxo_state'] if row else None
        except Exception as e:
            logger.error(f"Error getting UTxO state for {address}: {str(e)}")
            return None

async def get_wallet_for_user(user_id: str, address: str):
    """Get wallet details for a specific user and address
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        Record: Wallet record with all details or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            return row
        except Exception as e:
            logger.error(f"Error getting wallet for user {user_id} address {address}: {str(e)}")
            return None

async def is_token_change_processed(wallet_id: int, tx_hash: str) -> bool:
    """Check if a token change has been processed
    
    Args:
        wallet_id (int): Wallet ID
        tx_hash (str): Transaction hash
        
    Returns:
        bool: True if change was processed
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM transactions 
                    WHERE wallet_id = $1 AND tx_hash = $2
                )
                """,
                wallet_id, tx_hash
            )
            return bool(result)
    except Exception as e:
        logger.error(f"Error checking processed token change: {str(e)}")
        return False

async def add_processed_token_change(wallet_id: int, tx_hash: str) -> bool:
    """Add a processed token change
    
    Args:
        wallet_id (int): Wallet ID
        tx_hash (str): Transaction hash
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO transactions (wallet_id, tx_hash)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                wallet_id, tx_hash
            )
            return True
    except Exception as e:
        logger.error(f"Error adding processed token change: {str(e)}")
        return False

async def get_new_tokens(address: str) -> list:
    """Get new tokens added to a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        list: List of new token IDs
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            old_state = await get_utxo_state(address)
            if not old_state:
                return []
                
            old_tokens = set(old_state.get('tokens', {}).keys())
            current_state = await get_utxo_state(address)
            if not current_state:
                return []
                
            current_tokens = set(current_state.get('tokens', {}).keys())
            return list(current_tokens - old_tokens)
    except Exception as e:
        logger.error(f"Error getting new tokens: {str(e)}")
        return []

async def get_removed_nfts(address: str) -> list:
    """Get NFTs removed from a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        list: List of removed NFT IDs
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            old_state = await get_utxo_state(address)
            if not old_state:
                return []
                
            old_nfts = set(old_state.get('nfts', []))
            current_state = await get_utxo_state(address)
            if not current_state:
                return []
                
            current_nfts = set(current_state.get('nfts', []))
            return list(old_nfts - current_nfts)
    except Exception as e:
        logger.error(f"Error getting removed NFTs: {str(e)}")
        return []

async def get_yummi_warning_count(wallet_id: int) -> int:
    """Get the number of YUMMI warnings for a wallet
    
    Args:
        wallet_id (int): Wallet ID
        
    Returns:
        int: Number of warnings (0 if no warnings)
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                """
                SELECT warning_count 
                FROM yummi_warnings 
                WHERE wallet_id = $1
                """,
                wallet_id
            )
            return result or 0
    except Exception as e:
        logger.error(f"Error getting YUMMI warning count: {str(e)}")
        return 0

async def increment_yummi_warning(wallet_id: int) -> int:
    """Increment the YUMMI warning count for a wallet
    
    Args:
        wallet_id (int): Wallet ID
        
    Returns:
        int: New warning count
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                """
                INSERT INTO yummi_warnings (wallet_id, warning_count)
                VALUES ($1, 1)
                ON CONFLICT (wallet_id) DO UPDATE
                SET warning_count = yummi_warnings.warning_count + 1,
                    last_warning_at = CURRENT_TIMESTAMP
                RETURNING warning_count
                """,
                wallet_id
            )
            return result
    except Exception as e:
        logger.error(f"Error incrementing YUMMI warning: {str(e)}")
        return 0

async def reset_yummi_warning(wallet_id: int) -> bool:
    """Reset the YUMMI warning count for a wallet
    
    Args:
        wallet_id (int): Wallet ID
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM yummi_warnings
                WHERE wallet_id = $1
                """,
                wallet_id
            )
            return True
    except Exception as e:
        logger.error(f"Error resetting YUMMI warning: {str(e)}")
        return False

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
