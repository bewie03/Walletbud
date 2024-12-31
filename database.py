import os
import logging
import asyncpg
from datetime import datetime
import json
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

# Update schema version
SCHEMA_VERSION = "1.2.0"

SCHEMA = """
-- Drop tables in correct order
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS processed_rewards CASCADE;
DROP TABLE IF EXISTS yummi_warnings CASCADE;
DROP TABLE IF EXISTS stake_addresses CASCADE;
DROP TABLE IF EXISTS policy_expiry CASCADE;
DROP TABLE IF EXISTS wallets CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Users table for storing Discord users
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ada_transactions BOOLEAN DEFAULT TRUE,
    token_changes BOOLEAN DEFAULT TRUE,
    nft_updates BOOLEAN DEFAULT TRUE,
    staking_rewards BOOLEAN DEFAULT TRUE,
    stake_changes BOOLEAN DEFAULT TRUE,
    low_balance BOOLEAN DEFAULT TRUE,
    policy_expiry BOOLEAN DEFAULT TRUE,
    delegation_status BOOLEAN DEFAULT TRUE,
    dapp_interactions BOOLEAN DEFAULT TRUE
);

-- Wallets table for storing wallet addresses
CREATE TABLE IF NOT EXISTS wallets (
    id SERIAL PRIMARY KEY,
    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP,
    last_yummi_check TIMESTAMP,
    last_balance BIGINT,
    utxo_state JSONB,
    delegation_pool_id TEXT,
    last_dapp_tx TEXT,
    UNIQUE(user_id, address)
);

-- Create index on address for faster lookups
CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address);

-- Stake addresses table for tracking stake addresses
CREATE TABLE IF NOT EXISTS stake_addresses (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id) ON DELETE CASCADE,
    stake_address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, stake_address)
);

-- Create index on stake_address for faster lookups
CREATE INDEX IF NOT EXISTS idx_stake_addresses_stake_address ON stake_addresses(stake_address);

-- Processed rewards table
CREATE TABLE IF NOT EXISTS processed_rewards (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id) ON DELETE CASCADE,
    epoch INTEGER NOT NULL,
    amount BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, epoch)
);

-- Create index on wallet_id and epoch for faster lookups
CREATE INDEX IF NOT EXISTS idx_processed_rewards_wallet_epoch ON processed_rewards(wallet_id, epoch);

-- YUMMI warning table
CREATE TABLE IF NOT EXISTS yummi_warnings (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id) ON DELETE CASCADE,
    warning_count INTEGER DEFAULT 0,
    last_warning_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id)
);

-- Policy expiry table for tracking policy expiry
CREATE TABLE IF NOT EXISTS policy_expiry (
    policy_id TEXT PRIMARY KEY,
    expiry_slot INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on expiry_slot for faster lookups
CREATE INDEX IF NOT EXISTS idx_policy_expiry_expiry_slot ON policy_expiry(expiry_slot);

-- Transactions table for storing transactions
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id) ON DELETE CASCADE,
    tx_hash TEXT NOT NULL,
    metadata JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, tx_hash)
);

-- Create index on tx_hash for faster lookups
CREATE INDEX IF NOT EXISTS idx_transactions_tx_hash ON transactions(tx_hash);

-- Create index on wallet_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_transactions_wallet_id ON transactions(wallet_id);
"""

async def init_db():
    """Initialize database and create tables"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Start transaction
            async with conn.transaction():
                # Check if tables exist
                tables_exist = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'users'
                    )
                """)
                
                if not tables_exist:
                    logger.info("Tables don't exist, creating schema...")
                    await conn.execute(SCHEMA)
                    logger.info("Database initialized successfully")
                else:
                    logger.info("Tables already exist, skipping initialization")
                    
                    # Verify all required tables exist
                    required_tables = [
                        'users', 'wallets', 'stake_addresses', 'processed_rewards',
                        'yummi_warnings', 'policy_expiry', 'transactions'
                    ]
                    
                    for table in required_tables:
                        exists = await conn.fetchval(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = $1
                            )
                        """, table)
                        
                        if not exists:
                            logger.error(f"Missing required table: {table}")
                            raise Exception(f"Database schema is incomplete: missing {table} table")
                    
                    logger.info("All required tables verified")
        
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
    logger.info(f"Adding wallet {address[:20]}... for user {user_id}")
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # First, ensure user exists
            await conn.execute(
                """
                INSERT INTO users (user_id)
                VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_id
            )
            logger.debug(f"User {user_id} ensured in database")
            
            # Then add wallet
            await conn.execute(
                """
                INSERT INTO wallets (user_id, address)
                VALUES ($1, $2)
                ON CONFLICT (user_id, address) DO NOTHING
                """,
                user_id, address
            )
            logger.info(f"Wallet {address[:20]}... added successfully")
            return True
    except Exception as e:
        logger.error(f"Failed to add wallet {address[:20]}... for user {user_id}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def remove_wallet(user_id: str, address: str) -> bool:
    """Remove a wallet from monitoring
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address to remove
        
    Returns:
        bool: Success status
    """
    logger.info(f"Removing wallet {address[:20]}... for user {user_id}")
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
            success = result.split()[1] != '0'
            if success:
                logger.info(f"Wallet {address[:20]}... removed successfully")
            else:
                logger.warning(f"No wallet {address[:20]}... found for user {user_id}")
            return success
    except Exception as e:
        logger.error(f"Failed to remove wallet {address[:20]}... for user {user_id}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_wallet(user_id: str, address: str):
    """Get a specific wallet
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        Record: Wallet record or None if not found
    """
    logger.debug(f"Fetching wallet {address[:20]}... for user {user_id}")
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            record = await conn.fetchrow(
                """
                SELECT * FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            if record:
                logger.debug(f"Found wallet {address[:20]}... for user {user_id}")
            else:
                logger.debug(f"No wallet {address[:20]}... found for user {user_id}")
            return record
    except Exception as e:
        logger.error(f"Failed to fetch wallet {address[:20]}... for user {user_id}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def get_all_wallets() -> List[asyncpg.Record]:
    """Get all monitored wallets
    
    Returns:
        List[asyncpg.Record]: List of wallet records
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug("Fetching all wallets")
            return await conn.fetch("SELECT * FROM wallets")
    except Exception as e:
        logger.error(f"Error getting all wallets: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return []

async def get_all_wallets_for_user(user_id: str) -> List[str]:
    """Get all wallets for a specific user
    
    Args:
        user_id (str): Discord user ID
        
    Returns:
        List[str]: List of wallet addresses
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching wallets for user {user_id}")
            rows = await conn.fetch(
                "SELECT address FROM wallets WHERE user_id = $1",
                user_id
            )
            return [row['address'] for row in rows]
    except Exception as e:
        logger.error(f"Error getting user wallets: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Fetching user ID for wallet {address[:20]}...")
            return await conn.fetchval(
                "SELECT user_id FROM wallets WHERE address = $1",
                address
            )
    except Exception as e:
        logger.error(f"Error getting user ID: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def get_last_yummi_check(address: str) -> Optional[datetime]:
    """Get the last time YUMMI requirement was checked
    
    Args:
        address (str): Wallet address
        
    Returns:
        Optional[datetime]: Last check time or None if never checked
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching last YUMMI check for wallet {address[:20]}...")
            return await conn.fetchval(
                "SELECT last_yummi_check FROM wallets WHERE address = $1",
                address
            )
    except Exception as e:
        logger.error(f"Error getting last YUMMI check: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def update_last_yummi_check(address: str):
    """Update the last YUMMI check time
    
    Args:
        address (str): Wallet address
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating last YUMMI check for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")

async def update_last_checked(wallet_id: int):
    """Update the last checked timestamp
    
    Args:
        wallet_id (int): Wallet ID to update
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating last checked for wallet ID {wallet_id}...")
            query = """
                UPDATE wallets 
                SET last_checked = CURRENT_TIMESTAMP 
                WHERE id = $1
            """
            await conn.execute(query, wallet_id)
            return True
    except Exception as e:
        logger.error(f"Error updating last checked: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

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
            logger.debug(f"Fetching wallet ID for user {user_id} and address {address[:20]}...")
            return await conn.fetchval(
                """
                SELECT id FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
    except Exception as e:
        logger.error(f"Error getting wallet ID: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def add_transaction(wallet_id: int, tx_hash: str, metadata: dict = None) -> bool:
    """Add a transaction to the database with metadata
    
    Args:
        wallet_id (int): Wallet ID
        tx_hash (str): Transaction hash
        metadata (dict, optional): Transaction metadata. Defaults to None.
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Start transaction
            async with conn.transaction():
                # Add or update transaction
                query = """
                    INSERT INTO transactions (wallet_id, tx_hash, metadata)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (wallet_id, tx_hash) 
                    DO UPDATE SET 
                        metadata = EXCLUDED.metadata,
                        timestamp = CURRENT_TIMESTAMP
                    RETURNING id
                """
                
                logger.debug(f"Adding transaction {tx_hash[:20]}... for wallet ID {wallet_id}...")
                result = await conn.fetchval(
                    query, 
                    wallet_id, 
                    tx_hash, 
                    json.dumps(metadata) if metadata else None
                )
                
                if result:
                    logger.info(f"Added/updated transaction {tx_hash[:20]}... for wallet ID {wallet_id}")
                    return True
                else:
                    logger.error(f"Failed to add transaction {tx_hash[:20]}... for wallet ID {wallet_id}")
                    return False
                    
    except Exception as e:
        logger.error(f"Error adding transaction: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_transaction_metadata(wallet_id: int, tx_hash: str) -> Optional[dict]:
    """Get transaction metadata from the database
    
    Args:
        wallet_id (int): Wallet ID
        tx_hash (str): Transaction hash
        
    Returns:
        Optional[dict]: Transaction metadata or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching transaction metadata for wallet ID {wallet_id} and tx hash {tx_hash[:20]}...")
            query = """
                SELECT metadata
                FROM transactions
                WHERE wallet_id = $1 AND tx_hash = $2
            """
            result = await conn.fetchval(query, wallet_id, tx_hash)
            return json.loads(result) if result else None
    except Exception as e:
        logger.error(f"Error getting transaction metadata: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

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
            logger.debug(f"Fetching notification settings for user {user_id}...")
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE user_id = $1",
                user_id
            )
            if row:
                return {
                    "ada_transactions": row['ada_transactions'],
                    "token_changes": row['token_changes'],
                    "nft_updates": row['nft_updates'],
                    "staking_rewards": row['staking_rewards'],
                    "stake_changes": row['stake_changes'],
                    "low_balance": row['low_balance'],
                    "policy_expiry": row['policy_expiry'],
                    "delegation_status": row['delegation_status'],
                    "dapp_interactions": row['dapp_interactions']
                }
            else:
                return None
    except Exception as e:
        logger.error(f"Error getting notification settings: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            # Update specific setting
            query = f"""
                UPDATE users 
                SET {setting} = $1
                WHERE user_id = $2
            """
            logger.debug(f"Updating notification setting {setting} for user {user_id}...")
            result = await conn.execute(query, enabled, user_id)
            return result == "UPDATE 1"
    except Exception as e:
        logger.error(f"Error updating notification setting: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return True  # Default to notify on error

async def get_recent_transactions(address: str, hours: int = 1) -> List[asyncpg.Record]:
    """Get transactions in the last N hours
    
    Args:
        address (str): The wallet address
        hours (int): Number of hours to look back
        
    Returns:
        List[asyncpg.Record]: List of transactions
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching recent transactions for wallet {address[:20]}...")
            return await conn.fetch(
                """
                SELECT t.* FROM transactions t
                JOIN wallets w ON t.wallet_id = w.id
                WHERE w.address = $1
                AND t.timestamp > NOW() - interval '$2 hours'
                ORDER BY t.timestamp DESC
                """,
                address, hours
            )
    except Exception as e:
        logger.error(f"Error getting recent transactions: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Checking ADA balance for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Updating ADA balance for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Updating token balances for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Fetching wallet balance for {address[:20]}...")
            balance = await conn.fetchval(
                "SELECT last_balance FROM wallets WHERE address = $1",
                address
            )
            return balance if balance is not None else 0
    except Exception as e:
        logger.error(f"Error getting wallet balance: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Updating UTxO state for wallet {address[:20]}...")
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
            logger.error(f"Error updating UTxO state for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return False

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
            logger.debug(f"Fetching stake address for wallet {address[:20]}...")
            result = await conn.fetchval(
                """
                SELECT s.stake_address
                FROM stake_addresses s
                JOIN wallets w ON w.id = s.wallet_id
                WHERE w.address = $1
                ORDER BY s.updated_at DESC
                LIMIT 1
                """,
                address
            )
            return result
        except Exception as e:
            logger.error(f"Error getting stake address for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return None

async def update_stake_address(address: str, stake_address: str):
    """Update the stake address for a wallet
    
    Args:
        address (str): Wallet address
        stake_address (str): Stake address
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating stake address for wallet {address[:20]}...")
            wallet_id = await conn.fetchval(
                """
                SELECT id
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            
            if wallet_id is None:
                return False
                
            await conn.execute(
                """
                INSERT INTO stake_addresses (wallet_id, stake_address)
                VALUES ($1, $2)
                ON CONFLICT (wallet_id, stake_address) 
                DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                """,
                wallet_id,
                stake_address
            )
            return True
    except Exception as e:
        logger.error(f"Error updating stake address for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Checking if reward is processed for stake address {stake_address[:20]} and epoch {epoch}...")
            row = await conn.fetchrow(
                """
                SELECT id
                FROM processed_rewards
                WHERE wallet_id = (SELECT id FROM wallets WHERE stake_address = $1) AND epoch = $2
                """,
                stake_address, epoch
            )
            return bool(row)
        except Exception as e:
            logger.error(f"Error checking reward for {stake_address[:20]} epoch {epoch}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Adding processed reward for stake address {stake_address[:20]} and epoch {epoch}...")
            # First get the wallet ID from stake address
            wallet_id = await conn.fetchval(
                """
                SELECT id
                FROM wallets
                WHERE address IN (
                    SELECT address
                    FROM wallets w
                    JOIN stake_addresses s ON w.id = s.wallet_id
                    WHERE s.stake_address = $1
                )
                LIMIT 1
                """,
                stake_address
            )
            
            if wallet_id is None:
                logger.error(f"No wallet found for stake address {stake_address}")
                return False
            
            await conn.execute(
                """
                INSERT INTO processed_rewards (wallet_id, epoch, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (wallet_id, epoch) DO NOTHING
                """,
                wallet_id, epoch, amount
            )
            return True
        except Exception as e:
            logger.error(f"Error adding processed reward: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return False

async def get_last_transactions(address: str) -> List[str]:
    """Retrieve the last set of processed transactions for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        List[str]: List of transaction hashes
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            logger.debug(f"Fetching last transactions for wallet {address[:20]}...")
            wallet_id = await conn.fetchval(
                """
                SELECT id
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
            logger.error(f"Error getting last transactions for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return []

async def get_utxo_state(address: str) -> Optional[dict]:
    """Get the UTxO state for a wallet address
    
    Args:
        address (str): Wallet address
        
    Returns:
        Optional[dict]: Dictionary containing UTxO state or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            logger.debug(f"Fetching UTxO state for wallet {address[:20]}...")
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
            logger.error(f"Error getting UTxO state for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return None

async def get_wallet_for_user(user_id: str, address: str) -> Optional[asyncpg.Record]:
    """Get wallet details for a specific user and address
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        Optional[asyncpg.Record]: Wallet record with all details or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            logger.debug(f"Fetching wallet for user {user_id} and address {address[:20]}...")
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
            logger.error(f"Error getting wallet for user {user_id} address {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Checking if token change is processed for wallet ID {wallet_id} and tx hash {tx_hash[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Adding processed token change for wallet ID {wallet_id} and tx hash {tx_hash[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_new_tokens(address: str) -> List[str]:
    """Get new tokens added to a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        List[str]: List of new token IDs
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching new tokens for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return []

async def get_removed_nfts(address: str) -> List[str]:
    """Get NFTs removed from a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        List[str]: List of removed NFT IDs
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching removed NFTs for wallet {address[:20]}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Fetching YUMMI warning count for wallet ID {wallet_id}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Incrementing YUMMI warning count for wallet ID {wallet_id}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
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
            logger.debug(f"Resetting YUMMI warning count for wallet ID {wallet_id}...")
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
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_delegation_status(address: str):
    """Get the current delegation status for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        str: Pool ID or None if not delegated
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching delegation status for wallet {address[:20]}...")
            result = await conn.fetchval(
                """
                SELECT delegation_pool_id
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return result
    except Exception as e:
        logger.error(f"Error getting delegation status: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def update_delegation_status(address: str, pool_id: str):
    """Update the delegation status for a wallet
    
    Args:
        address (str): Wallet address
        pool_id (str): Pool ID
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating delegation status for wallet {address[:20]}...")
            await conn.execute(
                """
                UPDATE wallets
                SET delegation_pool_id = $2
                WHERE address = $1
                """,
                address,
                pool_id
            )
            return True
    except Exception as e:
        logger.error(f"Error updating delegation status: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_policy_expiry(policy_id: str):
    """Get the expiry time for a policy
    
    Args:
        policy_id (str): Policy ID
        
    Returns:
        int: Slot number when policy expires or None
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching policy expiry for policy ID {policy_id}...")
            result = await conn.fetchval(
                """
                SELECT expiry_slot
                FROM policy_expiry
                WHERE policy_id = $1
                """,
                policy_id
            )
            return result
    except Exception as e:
        logger.error(f"Error getting policy expiry: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def update_policy_expiry(policy_id: str, expiry_slot: int):
    """Update or insert policy expiry information
    
    Args:
        policy_id (str): Policy ID
        expiry_slot (int): Slot number when policy expires
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating policy expiry for policy ID {policy_id}...")
            await conn.execute(
                """
                INSERT INTO policy_expiry (policy_id, expiry_slot)
                VALUES ($1, $2)
                ON CONFLICT (policy_id) DO UPDATE
                SET expiry_slot = $2,
                    updated_at = CURRENT_TIMESTAMP
                """,
                policy_id,
                expiry_slot
            )
            return True
    except Exception as e:
        logger.error(f"Error updating policy expiry: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_dapp_interactions(address: str) -> str:
    """Get the last processed DApp interaction for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        str: Last processed tx hash or None
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            logger.debug(f"Fetching DApp interactions for wallet {address[:20]}...")
            row = await conn.fetchrow(
                """
                SELECT last_dapp_tx
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return row['last_dapp_tx'] if row else None
        except Exception as e:
            logger.error(f"Error getting DApp interactions for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return None

async def update_dapp_interaction(address: str, tx_hash: str) -> bool:
    """Update the last processed DApp interaction
    
    Args:
        address (str): Wallet address
        tx_hash (str): Transaction hash
        
    Returns:
        bool: Success status
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            logger.debug(f"Updating DApp interaction for wallet {address[:20]}...")
            await conn.execute(
                """
                UPDATE wallets
                SET last_dapp_tx = $2
                WHERE address = $1
                """,
                address, tx_hash
            )
            return True
        except Exception as e:
            logger.error(f"Error updating DApp interaction for {address[:20]}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return False

async def get_last_dapp_tx(address: str) -> Optional[str]:
    """Get the last processed DApp transaction for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        Optional[str]: Transaction hash or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Fetching last DApp transaction for wallet {address[:20]}...")
            query = """
                SELECT last_dapp_tx
                FROM wallets
                WHERE address = $1
            """
            result = await conn.fetchval(query, address)
            return result
    except Exception as e:
        logger.error(f"Error getting last DApp transaction: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def update_last_dapp_tx(address: str, tx_hash: str) -> bool:
    """Update the last processed DApp transaction for a wallet
    
    Args:
        address (str): Wallet address
        tx_hash (str): Transaction hash
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.debug(f"Updating last DApp transaction for wallet {address[:20]}...")
            query = """
                UPDATE wallets
                SET last_dapp_tx = $2
                WHERE address = $1
            """
            await conn.execute(query, address, tx_hash)
            return True
    except Exception as e:
        logger.error(f"Error updating last DApp transaction: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def initialize_notification_settings(user_id: str):
    """Initialize default notification settings for a new user
    
    Args:
        user_id (str): Discord user ID
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Default settings - all notifications enabled
            default_settings = {
                "ada_transactions": True,
                "token_changes": True,
                "nft_updates": True,
                "staking_rewards": True,
                "stake_changes": True,
                "low_balance": True,
                "policy_expiry": True,
                "delegation_status": True,
                "dapp_interactions": True
            }
            
            # Insert into users table with default settings
            await conn.execute(
                """
                INSERT INTO users (user_id, ada_transactions, token_changes, nft_updates, staking_rewards, stake_changes, low_balance, policy_expiry, delegation_status, dapp_interactions)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (user_id) DO UPDATE 
                SET ada_transactions = EXCLUDED.ada_transactions,
                    token_changes = EXCLUDED.token_changes,
                    nft_updates = EXCLUDED.nft_updates,
                    staking_rewards = EXCLUDED.staking_rewards,
                    stake_changes = EXCLUDED.stake_changes,
                    low_balance = EXCLUDED.low_balance,
                    policy_expiry = EXCLUDED.policy_expiry,
                    delegation_status = EXCLUDED.delegation_status,
                    dapp_interactions = EXCLUDED.dapp_interactions
                """,
                user_id,
                default_settings['ada_transactions'],
                default_settings['token_changes'],
                default_settings['nft_updates'],
                default_settings['staking_rewards'],
                default_settings['stake_changes'],
                default_settings['low_balance'],
                default_settings['policy_expiry'],
                default_settings['delegation_status'],
                default_settings['dapp_interactions']
            )
            
            logger.info(f"Initialized notification settings for user {user_id}")
                
    except Exception as e:
        logger.error(f"Error initializing notification settings: {e}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")

async def main():
    """Example usage of database functions"""
    try:
        await add_wallet("1234567890", "addr1...")
        print(await get_all_wallets())
        await remove_wallet("1234567890", "addr1...")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
