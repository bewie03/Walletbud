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
-- Drop existing tables
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS wallet_states;
DROP TABLE IF EXISTS processed_rewards;
DROP TABLE IF EXISTS stake_addresses;
DROP TABLE IF EXISTS removed_nfts;
DROP TABLE IF EXISTS new_tokens;
DROP TABLE IF EXISTS balance_alerts;
DROP TABLE IF EXISTS processed_token_changes;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notification_settings JSONB
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

-- Removed NFTs table
CREATE TABLE IF NOT EXISTS removed_nfts (
    address TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    removed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address, policy_id)
);

-- New tokens table
CREATE TABLE IF NOT EXISTS new_tokens (
    address TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address, policy_id)
);

-- Balance alerts table
CREATE TABLE IF NOT EXISTS balance_alerts (
    address TEXT NOT NULL,
    balance DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address, timestamp)
);

-- Token balance changes table
CREATE TABLE IF NOT EXISTS processed_token_changes (
    address TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    old_balance BIGINT NOT NULL,
    new_balance BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address, policy_id, processed_at)
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
                '''
                INSERT INTO users (user_id, notification_settings)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
                ''',
                user_id,
                json.dumps({
                    "ada_transactions": True,
                    "token_changes": True,
                    "nft_updates": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                })
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
                '''
                SELECT * FROM transactions
                WHERE wallet_id IN (
                    SELECT id FROM wallets WHERE address = $1
                )
                AND timestamp > NOW() - interval '$2 hours'
                ORDER BY timestamp DESC
                ''',
                address, hours
            )
            
    except Exception as e:
        logger.error(f"Error getting recent transactions: {str(e)}")
        return []

async def get_removed_nfts(address: str, prev_nfts: set, current_nfts: set) -> set:
    """Get NFTs that were removed from the wallet
    
    Args:
        address (str): The wallet address
        prev_nfts (set): Previous set of NFT policy IDs
        current_nfts (set): Current set of NFT policy IDs
        
    Returns:
        set: Set of removed NFT policy IDs
    """
    try:
        removed = prev_nfts - current_nfts
        if not removed:
            return set()
            
        # Store removed NFTs for tracking
        pool = await get_pool()
        async with pool.acquire() as conn:
            for nft in removed:
                await conn.execute(
                    '''
                    INSERT INTO removed_nfts (address, policy_id, removed_at)
                    VALUES ($1, $2, NOW())
                    ''',
                    address, nft
                )
        return removed
        
    except Exception as e:
        logger.error(f"Error tracking removed NFTs: {str(e)}")
        return set()

async def get_new_tokens(address: str, prev_tokens: set, current_tokens: set) -> set:
    """Get new tokens added to the wallet (excluding NFTs)
    
    Args:
        address (str): The wallet address
        prev_tokens (set): Previous set of token policy IDs
        current_tokens (set): Current set of token policy IDs
        
    Returns:
        set: Set of new token policy IDs
    """
    try:
        new_tokens = current_tokens - prev_tokens
        if not new_tokens:
            return set()
            
        # Store new tokens for tracking
        pool = await get_pool()
        async with pool.acquire() as conn:
            for token in new_tokens:
                await conn.execute(
                    '''
                    INSERT INTO new_tokens (address, policy_id, added_at)
                    VALUES ($1, $2, NOW())
                    ''',
                    address, token
                )
        return new_tokens
        
    except Exception as e:
        logger.error(f"Error tracking new tokens: {str(e)}")
        return set()

async def check_ada_balance(address: str, current_balance: int) -> tuple[bool, int]:
    """Check if ADA balance is below threshold
    
    Args:
        address (str): The wallet address
        current_balance (int): Current balance in lovelace
        
    Returns:
        tuple[bool, int]: (is_below_threshold, current_balance_ada)
    """
    try:
        # Convert lovelace to ADA
        balance_ada = current_balance / 1_000_000
        
        # Check if below threshold and not already notified
        pool = await get_pool()
        async with pool.acquire() as conn:
            last_alert = await conn.fetchval(
                '''
                SELECT timestamp FROM balance_alerts
                WHERE address = $1
                ORDER BY timestamp DESC
                LIMIT 1
                ''',
                address
            )
            
            # Only alert if no previous alert in last hour
            if balance_ada < 10 and (
                not last_alert or 
                (datetime.now() - last_alert).total_seconds() > 3600
            ):
                await conn.execute(
                    '''
                    INSERT INTO balance_alerts (address, balance, timestamp)
                    VALUES ($1, $2, NOW())
                    ''',
                    address, balance_ada
                )
                return True, balance_ada
                
        return False, balance_ada
        
    except Exception as e:
        logger.error(f"Error checking ADA balance: {str(e)}")
        return False, 0

async def get_wallet_balance(address: str) -> tuple[int, dict]:
    """Get wallet's ADA balance and token balances
    
    Args:
        address (str): Wallet address
        
    Returns:
        tuple[int, dict]: (ADA balance in lovelace, {token_policy: amount})
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            utxo_state = await conn.fetchval(
                'SELECT utxo_state FROM wallet_states WHERE address = $1',
                address
            )
            
            if not utxo_state:
                return 0, {}
                
            state = json.loads(utxo_state)
            ada_balance = 0
            token_balances = {}
            
            for utxo_data in state.values():
                for unit, quantity in utxo_data.items():
                    if unit == "lovelace":
                        ada_balance += int(quantity)
                    else:
                        token_balances[unit] = token_balances.get(unit, 0) + int(quantity)
                        
            return ada_balance, token_balances
            
    except Exception as e:
        logger.error(f"Error getting wallet balance: {str(e)}")
        return 0, {}

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
                '''
                INSERT INTO users (user_id, notification_settings)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
                ''',
                user_id,
                json.dumps({
                    "ada_transactions": True,
                    "token_changes": True,
                    "nft_updates": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                })
            )
            
            # Get settings
            row = await conn.fetchrow(
                'SELECT notification_settings FROM users WHERE user_id = $1',
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
                '''
                INSERT INTO users (user_id, notification_settings)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
                ''',
                user_id,
                json.dumps({
                    "ada_transactions": True,
                    "token_changes": True,
                    "nft_updates": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                })
            )
            
            # Update specific setting
            await conn.execute(
                '''
                UPDATE users 
                SET notification_settings = jsonb_set(
                    COALESCE(notification_settings, '{}'::jsonb),
                    ARRAY[$2],
                    $3::jsonb,
                    true
                )
                WHERE user_id = $1
                ''',
                user_id, 
                setting,
                json.dumps(enabled)
            )
            return True
            
    except Exception as e:
        logger.error(f"Error updating notification setting: {str(e)}")
        return False

async def is_token_change_processed(address: str, policy_id: str, old_balance: int, new_balance: int) -> bool:
    """Check if a token balance change has been processed
    
    Args:
        address (str): Wallet address
        policy_id (str): Token policy ID
        old_balance (int): Previous balance
        new_balance (int): New balance
        
    Returns:
        bool: Whether this change has been processed
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Check for this exact change in the last hour
            result = await conn.fetchval(
                '''
                SELECT EXISTS(
                    SELECT 1 FROM processed_token_changes
                    WHERE address = $1
                    AND policy_id = $2
                    AND old_balance = $3
                    AND new_balance = $4
                    AND processed_at > NOW() - interval '1 hour'
                )
                ''',
                address, policy_id, old_balance, new_balance
            )
            return bool(result)
            
    except Exception as e:
        logger.error(f"Error checking processed token change: {str(e)}")
        return False

async def add_processed_token_change(address: str, policy_id: str, old_balance: int, new_balance: int):
    """Add a token balance change to processed list
    
    Args:
        address (str): Wallet address
        policy_id (str): Token policy ID
        old_balance (int): Previous balance
        new_balance (int): New balance
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO processed_token_changes 
                (address, policy_id, old_balance, new_balance)
                VALUES ($1, $2, $3, $4)
                ''',
                address, policy_id, old_balance, new_balance
            )
            
    except Exception as e:
        logger.error(f"Error adding processed token change: {str(e)}")

async def get_wallet_for_user(user_id: str) -> str:
    """Get the wallet address for a user
    
    Args:
        user_id (str): Discord user ID
        
    Returns:
        str: Wallet address or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                '''
                SELECT address FROM wallets
                WHERE user_id = (
                    SELECT user_id FROM users
                    WHERE user_id = $1
                )
                LIMIT 1
                ''',
                user_id
            )
            return result
            
    except Exception as e:
        logger.error(f"Error getting wallet for user: {str(e)}")
        return None

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
