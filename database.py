import os
import json
import logging
import asyncio
import asyncpg
from datetime import datetime
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
_pool_lock = asyncio.Lock()

async def init_db():
    """Initialize database connection pool"""
    global _pool
    async with _pool_lock:
        if _pool is None:
            try:
                _pool = await asyncpg.create_pool(
                    dsn=os.getenv('DATABASE_URL'),
                    min_size=1,
                    max_size=20,
                    command_timeout=60,
                    server_settings={
                        'timezone': 'UTC',
                        'application_name': 'WalletBud'
                    }
                )
                logger.info("Database connection pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {str(e)}")
                raise

async def close_db():
    """Close database connection pool"""
    global _pool
    async with _pool_lock:
        if _pool is not None:
            await _pool.close()
            _pool = None
            logger.info("Database connection pool closed")

async def cleanup_pool():
    """Clean up the database connection pool"""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed")

async def get_pool() -> asyncpg.Pool:
    """Get database connection pool
    
    Returns:
        asyncpg.Pool: Database connection pool
    
    Raises:
        RuntimeError: If pool is not initialized
    """
    if _pool is None:
        await init_db()
    return _pool

# Database initialization SQL
INIT_SQL = """
DROP TABLE IF EXISTS wallets CASCADE;
DROP TABLE IF EXISTS notification_settings CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS token_balances CASCADE;
DROP TABLE IF EXISTS delegation_status CASCADE;
DROP TABLE IF EXISTS processed_rewards CASCADE;
DROP TABLE IF EXISTS policy_expiry CASCADE;
DROP TABLE IF EXISTS dapp_interactions CASCADE;
DROP TABLE IF EXISTS failed_transactions CASCADE;
DROP TABLE IF EXISTS stake_addresses CASCADE;
DROP TABLE IF EXISTS db_version CASCADE;

CREATE TABLE IF NOT EXISTS db_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS wallets (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    address TEXT NOT NULL,
    stake_address TEXT,
    ada_balance DECIMAL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_policy_check TIMESTAMP,
    monitoring_since TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, address)
);

CREATE TABLE IF NOT EXISTS notification_settings (
    user_id TEXT PRIMARY KEY,
    ada_transactions BOOLEAN DEFAULT TRUE,
    token_changes BOOLEAN DEFAULT TRUE,
    nft_updates BOOLEAN DEFAULT TRUE,
    staking_rewards BOOLEAN DEFAULT TRUE,
    stake_changes BOOLEAN DEFAULT TRUE,
    policy_expiry BOOLEAN DEFAULT TRUE,
    delegation_status BOOLEAN DEFAULT TRUE,
    dapp_interactions BOOLEAN DEFAULT TRUE,
    failed_transactions BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id),
    tx_hash TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    archived BOOLEAN DEFAULT FALSE,
    archived_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(wallet_id, tx_hash)
);

CREATE TABLE IF NOT EXISTS token_balances (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    token_id TEXT NOT NULL,
    balance DECIMAL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(address, token_id)
);

CREATE TABLE IF NOT EXISTS delegation_status (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(address)
);

CREATE TABLE IF NOT EXISTS processed_rewards (
    id SERIAL PRIMARY KEY,
    stake_address TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    amount DECIMAL NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stake_address, epoch)
);

CREATE TABLE IF NOT EXISTS policy_expiry (
    id SERIAL PRIMARY KEY,
    policy_id TEXT NOT NULL,
    expiry_slot BIGINT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(policy_id)
);

CREATE TABLE IF NOT EXISTS dapp_interactions (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(address, tx_hash)
);

CREATE TABLE IF NOT EXISTS failed_transactions (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id),
    tx_hash TEXT NOT NULL,
    error_type TEXT NOT NULL,
    error_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_id, tx_hash)
);

CREATE TABLE IF NOT EXISTS stake_addresses (
    stake_address TEXT PRIMARY KEY,
    last_pool_id TEXT,
    last_checked TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallets_user_id ON wallets(user_id);
CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address);
CREATE INDEX IF NOT EXISTS idx_transactions_wallet_id ON transactions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_archived ON transactions(archived, archived_at);
CREATE INDEX IF NOT EXISTS idx_token_balances_address ON token_balances(address);
CREATE INDEX IF NOT EXISTS idx_delegation_status_address ON delegation_status(address);
CREATE INDEX IF NOT EXISTS idx_processed_rewards_stake_address ON processed_rewards(stake_address);
CREATE INDEX IF NOT EXISTS idx_policy_expiry_policy_id ON policy_expiry(policy_id);
CREATE INDEX IF NOT EXISTS idx_dapp_interactions_address ON dapp_interactions(address);
CREATE INDEX IF NOT EXISTS idx_failed_transactions_wallet_id ON failed_transactions(wallet_id);

CREATE TABLE IF NOT EXISTS transactions_by_month (
    LIKE transactions INCLUDING ALL
) PARTITION BY RANGE (created_at);

DO $$
BEGIN
    FOR i IN 0..11 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS transactions_%s_%s PARTITION OF transactions_by_month
            FOR VALUES FROM (%L) TO (%L)',
            to_char(CURRENT_DATE + (interval '1 month' * i), 'YYYY'),
            to_char(CURRENT_DATE + (interval '1 month' * i), 'MM'),
            CURRENT_DATE + (interval '1 month' * i),
            CURRENT_DATE + (interval '1 month' * (i + 1))
        );
    END LOOP;
END $$;
"""

# Database version tracking
CURRENT_VERSION = 3

# Migration scripts
MIGRATIONS = {
    1: """
    -- Version 1: Initial schema
    INSERT INTO db_version (version) VALUES ($1);
    """,
    2: [
        """
        -- Version 2: Add last_policy_check column to wallets table
        ALTER TABLE wallets
        ADD COLUMN IF NOT EXISTS last_policy_check TIMESTAMP;
        """,
        """
        UPDATE db_version SET version = $1 WHERE version = $1 - 1;
        """
    ],
    3: [
        """
        -- Version 3: Remove asset_history column from notification_settings table
        ALTER TABLE notification_settings 
        DROP COLUMN IF EXISTS asset_history;
        """,
        """
        UPDATE db_version SET version = $1 WHERE version = $1 - 1;
        """
    ]
}

async def get_db_version() -> int:
    """Get current database version"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Check if version table exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'db_version'
                )
            """)
            
            if not exists:
                return 0
                
            version = await conn.fetchval("SELECT version FROM db_version")
            return version or 0
            
    except Exception as e:
        logger.error(f"Failed to get database version: {str(e)}")
        return 0

async def run_migrations():
    """Run any pending database migrations"""
    try:
        current = await get_db_version()
        logger.info(f"Current database version: {current}")
        
        if current >= CURRENT_VERSION:
            logger.info("Database is up to date")
            return True
            
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Reset version if needed
                if current > 0:
                    await conn.execute("DELETE FROM db_version")
                    current = 0
                    logger.info("Reset database version")
                
                # Run all migrations in order
                for version in range(current + 1, CURRENT_VERSION + 1):
                    if version in MIGRATIONS:
                        logger.info(f"Running migration to version {version}")
                        migration = MIGRATIONS[version]
                        if isinstance(migration, list):
                            for statement in migration[:-1]:  # Execute all but the last statement
                                await conn.execute(statement)
                            # Execute the version update statement with parameter
                            await conn.execute(migration[-1], version)
                        else:
                            await conn.execute(migration, version)
                        logger.info(f"Completed migration to version {version}")
                    
                logger.info("All migrations completed successfully")
                return True
                
    except Exception as e:
        logger.error(f"Failed to run migrations: {str(e)}")
        return False

async def init_db():
    """Initialize database and run migrations"""
    try:
        # Get connection pool
        pool = await get_pool()
        
        # Create db_version table if it doesn't exist
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS db_version (
                    version INTEGER PRIMARY KEY
                );
            """)
        
        # Run migrations
        if not await run_migrations():
            raise Exception("Failed to run database migrations")
        
        # Verify all required tables exist
        async with pool.acquire() as conn:
            required_tables = [
                'wallets', 'notification_settings', 'transactions', 'token_balances',
                'delegation_status', 'processed_rewards', 'policy_expiry', 'dapp_interactions',
                'failed_transactions', 'db_version', 'stake_addresses'
            ]
            
            for table in required_tables:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = $1
                    )
                """, table)
                
                if not exists:
                    raise Exception(f"Missing required table: {table}")
            
            logger.info("All required tables verified")
            
            # Set up connection pool with safe settings
            await conn.execute("""
                SET SESSION work_mem = '50MB';  -- More memory for complex queries
                SET SESSION maintenance_work_mem = '256MB';  -- More memory for maintenance
                SET SESSION random_page_cost = 1.1;  -- Assume SSD storage
                SET SESSION max_parallel_workers_per_gather = 4;  -- Use parallel queries
                SET SESSION max_parallel_workers = 8;  -- Maximum parallel workers
            """)
            
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

async def add_wallet(user_id: str, address: str, stake_address: str = None) -> bool:
    """Add a wallet to monitor
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address to monitor
        stake_address (str): Stake address
        
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
                INSERT INTO wallets (user_id, address, stake_address, monitoring_since)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (user_id, address) DO NOTHING
                """,
                user_id, address, stake_address
            )
            logger.debug(f"User {user_id} ensured in database")
            
            # Then add wallet
            await conn.execute(
                """
                INSERT INTO notification_settings (user_id)
                VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_id
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
                "SELECT last_updated FROM wallets WHERE address = $1",
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
                SET last_updated = NOW()
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
                SET last_updated = CURRENT_TIMESTAMP 
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
                        created_at = CURRENT_TIMESTAMP
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
                "SELECT * FROM notification_settings WHERE user_id = $1",
                user_id
            )
            if row:
                return {
                    "ada_transactions": row['ada_transactions'],
                    "token_changes": row['token_changes'],
                    "nft_updates": row['nft_updates'],
                    "staking_rewards": row['staking_rewards'],
                    "stake_changes": row['stake_changes'],
                    "policy_expiry": row['policy_expiry'],
                    "delegation_status": row['delegation_status'],
                    "dapp_interactions": row['dapp_interactions'],
                    "failed_transactions": row['failed_transactions']
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
                UPDATE notification_settings 
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
    """Check if a user should be notified about a specific event type"""
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                f"""
                SELECT {notification_type} 
                FROM notification_settings 
                WHERE user_id = $1
                """,
                user_id
            )
            return bool(result)
    except Exception as e:
        logger.error(f"Error checking notification settings: {str(e)}")
        return True  # Default to notifying if there's an error

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
                AND t.created_at > NOW() - interval '$2 hours'
                ORDER BY t.created_at DESC
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
                "SELECT ada_balance FROM wallets WHERE address = $1",
                address
            )
            
            if balance is None:
                return False, 0
                
            balance_ada = balance
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
                SET ada_balance = $2 
                WHERE address = $1
                """,
                address, balance
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
            for token_id, balance in token_balances.items():
                await conn.execute(
                    """
                    INSERT INTO token_balances (address, token_id, balance)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (address, token_id) DO UPDATE
                    SET balance = $3
                    """,
                    address, token_id, balance
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
                "SELECT ada_balance FROM wallets WHERE address = $1",
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
    """Get cached stake address for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        Optional[str]: Stake address if cached, None otherwise
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT stake_address
            FROM wallets
            WHERE address = $1 AND stake_address IS NOT NULL
            """,
            address
        )
        return row['stake_address'] if row else None

async def update_stake_address(address: str, stake_address: str):
    """Update stake address for a wallet
    
    Args:
        address (str): Wallet address
        stake_address (str): Stake address
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE wallets
            SET stake_address = $2
            WHERE address = $1
            """,
            address,
            stake_address
        )

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
                WHERE stake_address = $1 AND epoch = $2
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
            await conn.execute(
                """
                INSERT INTO processed_rewards (stake_address, epoch, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (stake_address, epoch) DO NOTHING
                """,
                stake_address, epoch, amount
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
                ORDER BY created_at DESC
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

async def get_wallet_for_user(user_id: str, address: str) -> Optional[dict]:
    """Get wallet details for a specific user and address
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address
        
    Returns:
        Optional[dict]: Wallet record with all details or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            wallet = await conn.fetchrow(
                """
                SELECT * FROM wallets 
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            if wallet:
                return {
                    'id': wallet['id'],
                    'user_id': wallet['user_id'],
                    'address': wallet['address'],
                    'stake_address': wallet['stake_address'],
                    'ada_balance': wallet['ada_balance'],
                    'last_updated': wallet['last_updated'],
                    'last_policy_check': wallet['last_policy_check']
                }
            return None
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
                SELECT pool_id
                FROM delegation_status
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
                INSERT INTO delegation_status (address, pool_id)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET pool_id = $2
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
                SET expiry_slot = $2
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
                SELECT tx_hash
                FROM dapp_interactions
                WHERE address = $1
                ORDER BY created_at DESC
                LIMIT 1
                """,
                address
            )
            return row['tx_hash'] if row else None
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
                INSERT INTO dapp_interactions (address, tx_hash)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET tx_hash = $2
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
                SELECT tx_hash
                FROM dapp_interactions
                WHERE address = $1
                ORDER BY created_at DESC
                LIMIT 1
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
                INSERT INTO dapp_interactions (address, tx_hash)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET tx_hash = $2
            """
            await conn.execute(query, address, tx_hash)
            return True
    except Exception as e:
        logger.error(f"Error updating last DApp transaction: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def add_failed_transaction(wallet_id: int, tx_hash: str, error_type: str, error_details: dict) -> bool:
    """Add a failed transaction to the database"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO failed_transactions (wallet_id, tx_hash, error_type, error_details)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (wallet_id, tx_hash) 
                DO UPDATE SET error_type = $3, error_details = $4
                """,
                wallet_id, tx_hash, error_type, error_details
            )
            return True
    except Exception as e:
        logger.error(f"Error adding failed transaction: {str(e)}")
        return False

async def add_asset_history(
    wallet_id: int, 
    asset_id: str, 
    tx_hash: str, 
    action: str, 
    quantity: float,
    metadata: dict = None
) -> bool:
    """Add an asset history entry to the database"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO asset_history 
                (wallet_id, asset_id, tx_hash, action, quantity, metadata)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (wallet_id, asset_id, tx_hash) 
                DO UPDATE SET 
                    action = $4,
                    quantity = $5,
                    metadata = $6
                """,
                wallet_id, asset_id, tx_hash, action, quantity, metadata
            )
            return True
    except Exception as e:
        logger.error(f"Error adding asset history: {str(e)}")
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
                "policy_expiry": True,
                "delegation_status": True,
                "dapp_interactions": True,
                "failed_transactions": True
            }
            
            # Insert into users table with default settings
            await conn.execute(
                """
                INSERT INTO notification_settings (user_id, ada_transactions, token_changes, nft_updates, staking_rewards, stake_changes, policy_expiry, delegation_status, dapp_interactions, failed_transactions)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (user_id) DO UPDATE 
                SET ada_transactions = EXCLUDED.ada_transactions,
                    token_changes = EXCLUDED.token_changes,
                    nft_updates = EXCLUDED.nft_updates,
                    staking_rewards = EXCLUDED.staking_rewards,
                    stake_changes = EXCLUDED.stake_changes,
                    policy_expiry = EXCLUDED.policy_expiry,
                    delegation_status = EXCLUDED.delegation_status,
                    dapp_interactions = EXCLUDED.dapp_interactions,
                    failed_transactions = EXCLUDED.failed_transactions
                """,
                user_id,
                default_settings['ada_transactions'],
                default_settings['token_changes'],
                default_settings['nft_updates'],
                default_settings['staking_rewards'],
                default_settings['stake_changes'],
                default_settings['policy_expiry'],
                default_settings['delegation_status'],
                default_settings['dapp_interactions'],
                default_settings['failed_transactions']
            )
            
            logger.info(f"Initialized notification settings for user {user_id}")
                
    except Exception as e:
        logger.error(f"Error initializing notification settings: {e}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")

async def get_user_id_for_stake_address(stake_address: str) -> Optional[str]:
    """Get the user ID associated with a stake address
    
    Args:
        stake_address (str): Stake address
        
    Returns:
        Optional[str]: Discord user ID or None if not found
    """
    try:
        pool = await get_pool()
        query = """
            SELECT DISTINCT user_id 
            FROM wallets 
            WHERE stake_address = $1
            LIMIT 1
        """
        result = await pool.fetchval(query, stake_address)
        return result
    except Exception as e:
        logger.error(f"Error getting user ID for stake address: {str(e)}")
        return None

async def get_last_policy_check(address: str):
    """Get the last time policy expiry was checked
    
    Args:
        address (str): Wallet address
        
    Returns:
        Optional[datetime]: Last check time or None if never checked
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT last_policy_check
            FROM wallets
            WHERE address = $1
            """,
            address
        )
        return row['last_policy_check'] if row else None

async def update_last_policy_check(address: str):
    """Update the last policy check time
    
    Args:
        address (str): Wallet address
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE wallets
            SET last_policy_check = NOW()
            WHERE address = $1
            """,
            address
        )

async def get_monitoring_since(address: str) -> datetime:
    """Get when monitoring started for a wallet
    
    Args:
        address (str): Wallet address
        
    Returns:
        datetime: When monitoring started
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT monitoring_since
            FROM wallets
            WHERE address = $1
            """,
            address
        )

async def get_all_monitored_addresses():
    """Get all monitored addresses and stake addresses"""
    try:
        addresses = []
        stake_addresses = []
        
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Get all addresses and stake addresses
                query = 'SELECT address, stake_address FROM wallets'
                rows = await conn.fetch(query)
                
                for row in rows:
                    addresses.append(row['address'])
                    if row['stake_address']:
                        stake_addresses.append(row['stake_address'])
                        
        return addresses, stake_addresses
        
    except Exception as e:
        logger.error(f"Error getting monitored addresses: {str(e)}")
        return [], []

async def get_addresses_for_stake(stake_address: str) -> list[str]:
    """Get all addresses for a stake address"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.execute(
                """
                SELECT address
                FROM wallets
                WHERE stake_address = $1
                """,
                stake_address
            ) as cursor:
                return [row[0] async for row in cursor]
    except Exception as e:
        logger.error(f"Error getting addresses for stake: {str(e)}")
        return []

async def update_pool_for_stake(stake_address: str, pool_id: str):
    """Update the pool ID for a stake address"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                '''
                UPDATE stake_addresses 
                SET last_pool_id = ?, last_checked = CURRENT_TIMESTAMP 
                WHERE stake_address = ?
                ''',
                pool_id, stake_address
            )
            await conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error updating pool for stake address: {str(e)}")
        return False

async def get_wallet_info(address: str) -> dict:
    """Get wallet information including stake address"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.execute(
                """
                SELECT w.address, w.user_id, w.stake_address, w.monitoring_since,
                       s.last_pool_id, s.last_checked
                FROM wallets w
                LEFT JOIN stake_addresses s ON w.stake_address = s.stake_address
                WHERE w.address = $1
                """,
                address
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'address': row[0],
                        'user_id': row[1],
                        'stake_address': row[2],
                        'monitoring_since': row[3],
                        'last_pool_id': row[4],
                        'last_checked': row[5]
                    }
                return None
    except Exception as e:
        logger.error(f"Error getting wallet info: {str(e)}")
        return None

async def get_user_wallets(user_id: int) -> list:
    """Get all wallets for a user with their stake and delegation info"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.execute(
                """
                SELECT w.address, w.stake_address, w.monitoring_since,
                       s.last_pool_id, s.last_checked
                FROM wallets w
                LEFT JOIN stake_addresses s ON w.stake_address = s.stake_address
                WHERE w.user_id = $1
                ORDER BY w.monitoring_since DESC
                """,
                user_id
            ) as cursor:
                return [
                    {
                        'address': row[0],
                        'stake_address': row[1],
                        'monitoring_since': row[2],
                        'last_pool_id': row[3],
                        'last_checked': row[4]
                    }
                    async for row in cursor
                ]
    except Exception as e:
        logger.error(f"Error getting user wallets: {str(e)}")
        return []

async def update_stake_pool(stake_address: str, pool_id: str) -> bool:
    """Update stake pool for an address"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO stake_addresses (stake_address, last_pool_id, last_checked)
                VALUES ($1, $2, NOW())
                ON CONFLICT (stake_address) 
                DO UPDATE SET 
                    last_pool_id = EXCLUDED.last_pool_id,
                    last_checked = NOW()
                """,
                stake_address, pool_id
            )
            return True
    except Exception as e:
        logger.error(f"Error updating stake pool: {str(e)}")
        return False

async def add_wallet_for_user(user_id: str, address: str, stake_address: str = None):
    """Add a wallet to monitor
    
    Args:
        user_id (str): Discord user ID
        address (str): Wallet address to monitor
        stake_address (str): Stake address
        
    Returns:
        bool: Success status
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO wallets (user_id, address, stake_address)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, address) DO NOTHING
                """,
                user_id, address, stake_address
            )
            return True
    except Exception as e:
        logger.error(f"Failed to add wallet: {str(e)}")
        return False

async def remove_wallet_for_user(user_id: str, address: str):
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
            await conn.execute(
                """
                DELETE FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            return True
    except Exception as e:
        logger.error(f"Failed to remove wallet: {str(e)}")
        return False

async def update_notification_settings(user_id: str, setting: str, enabled: bool):
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
            await conn.execute(
                f"""
                UPDATE notification_settings
                SET {setting} = $1
                WHERE user_id = $2
                """,
                enabled, user_id
            )
            return True
    except Exception as e:
        logger.error(f"Failed to update notification setting: {str(e)}")
        return False

async def get_user_id_for_stake_address(stake_address: str) -> Optional[str]:
    """Get user ID associated with a stake address
    
    Args:
        stake_address (str): Stake address to look up
        
    Returns:
        Optional[str]: Discord user ID or None if not found
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                '''
                SELECT DISTINCT user_id 
                FROM wallets 
                WHERE stake_address = $1
                ''',
                stake_address
            )
            return str(row['user_id']) if row else None
    except Exception as e:
        logger.error(f"Error getting user ID for stake address {stake_address}: {str(e)}")
        return None

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
