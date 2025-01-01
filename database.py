import os
import json
import logging
import asyncio
import asyncpg
import asyncpg.exceptions
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Union, Tuple
from dotenv import load_dotenv
import re
import ssl
import certifi
from config import (
    DB_CONFIG,
    MAX_RETRIES,
    RATE_LIMIT_COOLDOWN,
    RATE_LIMIT_WINDOW,
    RATE_LIMIT_MAX_REQUESTS,
    BATCH_SIZE,
    ARCHIVE_AFTER_DAYS,
    DELETE_AFTER_DAYS
)

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Add input validation for wallet addresses
from cardano.address_validation import validate_cardano_address

def validate_address(address: str, address_type: str = 'wallet') -> bool:
    """Validate Cardano address format"""
    return validate_cardano_address(address)

# Pool management with proper locking and recreation
_pool_creation_time = None
_pool_max_age = timedelta(hours=1)  # Recreate pool every hour
_pool_lock = asyncio.Lock()
_pool = None
_last_error_time = None
_error_threshold = timedelta(minutes=5)  # Time window for error counting
_error_count = 0
_max_errors = 3  # Max errors before forcing pool recreation

def should_recreate_pool(current_time):
    """Determine if pool should be recreated based on age and errors"""
    pool_age = (current_time - _pool_creation_time) if _pool_creation_time else timedelta.max
    error_window = (current_time - _last_error_time) if _last_error_time else timedelta.max
    
    return (
        not _pool or  # No pool exists
        pool_age > _pool_max_age or  # Pool is too old
        (_error_count >= _max_errors and error_window < _error_threshold) or  # Too many recent errors
        (_error_count > 0 and error_window < timedelta(seconds=30))  # Any error in last 30s
    )

async def get_pool():
    """Get database connection pool with proper PostgreSQL configuration"""
    global _pool, _pool_creation_time, _last_error_time, _error_count

    async with _pool_lock:
        try:
            current_time = datetime.now()
            
            if should_recreate_pool(current_time):
                logger.info("Creating/recreating database connection pool")
                
                # Clean up old pool if it exists
                if _pool:
                    try:
                        await _pool.close()
                    except Exception as e:
                        logger.warning(f"Error closing old pool: {e}")
                
                # Get database URL with proper configuration
                db_url = get_database_url()
                
                # Create new pool with proper configuration
                _pool = await asyncpg.create_pool(
                    db_url,
                    min_size=DB_CONFIG['MIN_POOL_SIZE'],
                    max_size=DB_CONFIG['MAX_POOL_SIZE'],
                    max_inactive_connection_lifetime=DB_CONFIG['MAX_INACTIVE_CONNECTION_LIFETIME'],
                    command_timeout=DB_CONFIG['COMMAND_TIMEOUT'],
                    server_settings={
                        'application_name': 'walletbud',
                        'statement_timeout': str(DB_CONFIG['COMMAND_TIMEOUT'] * 1000),
                        'idle_in_transaction_session_timeout': '300000',  # 5 minutes
                        'client_encoding': 'UTF8'
                    }
                )
                
                # Reset error tracking
                _pool_creation_time = current_time
                _last_error_time = None
                _error_count = 0
                
                logger.info("Successfully created new connection pool")
            
            return _pool
            
        except Exception as e:
            # Handle connection errors
            _error_count += 1
            _last_error_time = current_time
            
            logger.error(f"Error in get_pool: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            
            # Force pool recreation on next attempt
            _pool = None
            raise

def get_database_url():
    """Get and validate database URL with proper Heroku postgres:// to postgresql:// conversion"""
    db_url = DATABASE_URL
    
    # Convert Heroku postgres:// to postgresql://
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    
    # Add sslmode=prefer if not specified
    if '?' not in db_url:
        db_url += '?sslmode=prefer'
    elif 'sslmode=' not in db_url:
        db_url += '&sslmode=prefer'
    
    return db_url

# Core table creation SQL
CREATE_TABLES_SQL = """
-- Create wallets table
CREATE TABLE IF NOT EXISTS wallets (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    address TEXT NOT NULL,
    stake_address TEXT,
    ada_balance NUMERIC DEFAULT 0,
    monitoring_since TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    last_policy_check TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id, address)
);

-- Create notification_settings table
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

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL,
    wallet_id INTEGER REFERENCES wallets(id),
    tx_hash TEXT NOT NULL,
    metadata JSONB,
    archived BOOLEAN DEFAULT FALSE,
    archived_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Create partitions for transactions
DO $$
BEGIN
    FOR y IN 2025..2025 LOOP
        FOR m IN 1..12 LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS transactions_%s_%s PARTITION OF transactions
                FOR VALUES FROM (%L) TO (%L)',
                y, LPAD(m::text, 2, '0'),
                format('%s-%s-01 00:00:00+00', y, LPAD(m::text, 2, '0')),
                format('%s-%s-01 00:00:00+00', 
                    CASE WHEN m = 12 THEN y + 1 ELSE y END,
                    CASE WHEN m = 12 THEN '01' ELSE LPAD((m + 1)::text, 2, '0') END
                )
            );
        END LOOP;
    END LOOP;
END $$;

-- Create failed_transactions table
CREATE TABLE IF NOT EXISTS failed_transactions (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id),
    tx_hash TEXT NOT NULL,
    error_type TEXT,
    error_details JSONB,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create asset_history table
CREATE TABLE IF NOT EXISTS asset_history (
    id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(id),
    asset_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    asset_name TEXT,
    quantity NUMERIC,
    action TEXT,
    tx_hash TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create token_balances table
CREATE TABLE IF NOT EXISTS token_balances (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    token_id TEXT NOT NULL,
    balance NUMERIC DEFAULT 0,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create delegation_status table
CREATE TABLE IF NOT EXISTS delegation_status (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create processed_rewards table
CREATE TABLE IF NOT EXISTS processed_rewards (
    id SERIAL PRIMARY KEY,
    stake_address TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    amount NUMERIC NOT NULL,
    processed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create policy_expiry table
CREATE TABLE IF NOT EXISTS policy_expiry (
    id SERIAL PRIMARY KEY,
    policy_id TEXT NOT NULL,
    expiry_slot BIGINT NOT NULL,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create dapp_interactions table
CREATE TABLE IF NOT EXISTS dapp_interactions (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create stake_addresses table
CREATE TABLE IF NOT EXISTS stake_addresses (
    stake_address TEXT PRIMARY KEY,
    last_pool_id TEXT,
    last_checked TIMESTAMP WITHOUT TIME ZONE
);
"""

# Migration tables creation SQL
CREATE_MIGRATION_TABLES = """
-- Drop existing tables to ensure clean state
DROP TABLE IF EXISTS migration_history CASCADE;
DROP TABLE IF EXISTS db_version CASCADE;

-- Create migration history table
CREATE TABLE migration_history (
    id SERIAL PRIMARY KEY,
    version INTEGER NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    success BOOLEAN DEFAULT TRUE,
    error TEXT
);

-- Create database version table
CREATE TABLE db_version (
    id SERIAL PRIMARY KEY,
    version INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

# Create timestamp update function
CREATE_TIMESTAMP_FUNCTION = """
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';
"""

# Create triggers for timestamp updates
CREATE_TRIGGERS_SQL = """
-- Create trigger for wallets
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_wallets_updated_at') THEN
        CREATE TRIGGER update_wallets_updated_at 
            BEFORE UPDATE ON wallets 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Create trigger for notification_settings
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_notification_settings_updated_at') THEN
        CREATE TRIGGER update_notification_settings_updated_at 
            BEFORE UPDATE ON notification_settings 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Create trigger for transactions
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_transactions_updated_at') THEN
        CREATE TRIGGER update_transactions_updated_at 
            BEFORE UPDATE ON transactions 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Create trigger for failed_transactions
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_failed_transactions_updated_at') THEN
        CREATE TRIGGER update_failed_transactions_updated_at 
            BEFORE UPDATE ON failed_transactions 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Create trigger for asset_history
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_asset_history_updated_at') THEN
        CREATE TRIGGER update_asset_history_updated_at 
            BEFORE UPDATE ON asset_history 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;
"""

async def create_indices(conn):
    """Create database indices with proper error handling"""
    indices = [
        # Wallets indices
        "CREATE INDEX IF NOT EXISTS idx_wallets_user_id ON wallets(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address)",
        "CREATE INDEX IF NOT EXISTS idx_wallets_stake_address ON wallets(stake_address)",
        "CREATE INDEX IF NOT EXISTS idx_wallets_last_updated ON wallets(last_updated DESC)",
        
        # Notification settings indices
        "CREATE INDEX IF NOT EXISTS idx_notification_settings_user_id ON notification_settings(user_id)",
        
        # Transactions indices
        "CREATE INDEX IF NOT EXISTS idx_transactions_wallet_tx ON transactions(wallet_id, tx_hash)",
        "CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at DESC)",
        
        # Failed transactions indices
        "CREATE INDEX IF NOT EXISTS idx_failed_transactions_wallet ON failed_transactions(wallet_id)",
        "CREATE INDEX IF NOT EXISTS idx_failed_transactions_created ON failed_transactions(created_at DESC)",
        
        # Asset history indices
        "CREATE INDEX IF NOT EXISTS idx_asset_history_wallet ON asset_history(wallet_id)",
        "CREATE INDEX IF NOT EXISTS idx_asset_history_asset ON asset_history(asset_id)",
        "CREATE INDEX IF NOT EXISTS idx_asset_history_created ON asset_history(created_at DESC)",
        
        # Token balances indices
        "CREATE INDEX IF NOT EXISTS idx_token_balances_address ON token_balances(address)",
        "CREATE INDEX IF NOT EXISTS idx_token_balances_token ON token_balances(token_id)",
        
        # Delegation status indices
        "CREATE INDEX IF NOT EXISTS idx_delegation_status_address ON delegation_status(address)",
        
        # Processed rewards indices
        "CREATE INDEX IF NOT EXISTS idx_processed_rewards_stake_address ON processed_rewards(stake_address)",
        "CREATE INDEX IF NOT EXISTS idx_processed_rewards_epoch ON processed_rewards(epoch)",
        
        # Policy expiry indices
        "CREATE INDEX IF NOT EXISTS idx_policy_expiry_policy_id ON policy_expiry(policy_id)",
        
        # DApp interactions indices
        "CREATE INDEX IF NOT EXISTS idx_dapp_interactions_address ON dapp_interactions(address)",
        
        # Stake addresses indices
        "CREATE INDEX IF NOT EXISTS idx_stake_addresses_stake_address ON stake_addresses(stake_address)",
    ]
    
    for index_sql in indices:
        try:
            await conn.execute(index_sql)
        except Exception as e:
            logger.warning(f"Error creating index: {e}")
            # Continue with other indices even if one fails

async def init_db():
    """Initialize the database with proper error handling"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            try:
                # Step 1: Create migration tables
                logger.info("Creating migration tables...")
                await conn.execute(CREATE_MIGRATION_TABLES)
                
                # Step 2: Create timestamp function
                logger.info("Creating timestamp update function...")
                await conn.execute(CREATE_TIMESTAMP_FUNCTION)
                
                # Step 3: Create core tables
                logger.info("Creating database tables...")
                await conn.execute(CREATE_TABLES_SQL)
                
                # Step 4: Create triggers
                logger.info("Creating timestamp update triggers...")
                await conn.execute(CREATE_TRIGGERS_SQL)
                
                # Step 5: Initialize version if not exists
                logger.info("Initializing database version...")
                await conn.execute("""
                    INSERT INTO db_version (version, created_at, last_updated)
                    SELECT 0, NOW(), NOW()
                    WHERE NOT EXISTS (SELECT 1 FROM db_version);
                """)
                
                # Step 6: Run migrations
                logger.info("Running database migrations...")
                current_version = await get_db_version(conn)
                if current_version < CURRENT_VERSION:
                    await migrate_database(conn)
                
                # Step 7: Initialize notification settings for existing users
                logger.info("Initializing notification settings...")
                users = await conn.fetch(
                    "SELECT DISTINCT user_id FROM wallets WHERE user_id IS NOT NULL"
                )
                for user in users:
                    await initialize_notification_settings(user['user_id'])
                
                logger.info("Database initialization completed successfully")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error during database initialization step: {error_msg}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                raise DatabaseError(f"Failed to initialize database: {error_msg}")
            
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise DatabaseError(f"Failed to initialize database: {e}")

# Enhanced retry logic with exponential backoff
RETRY_DELAYS = [1, 2, 5, 10, 30]  # Exponential backoff delays

async def execute_with_retry(func, *args, retries=None):
    """Execute database operation with retry logic and proper error handling"""
    if retries is None:
        retries = DB_CONFIG['RETRY_ATTEMPTS']
        
    last_error = None
    
    for attempt, delay in enumerate(RETRY_DELAYS[:retries]):
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await func(conn, *args)
                    
        except (asyncpg.ConnectionDoesNotExistError, 
                asyncpg.ConnectionFailureError) as e:
            last_error = e
            logger.warning(f"Connection error (attempt {attempt + 1}/{retries}): {e}")
            
            # Reset pool on connection errors
            await reset_pool()
            
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                
        except asyncpg.InterfaceError as e:
            last_error = e
            logger.error(f"Interface error: {e}")
            break  # Don't retry on interface errors
            
        except Exception as e:
            last_error = e
            logger.error(f"Database error (attempt {attempt + 1}/{retries}): {e}")
            
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            
    raise QueryError(f"Database operation failed after {retries} attempts: {str(last_error)}")

async def fetch_all(query: str, *args, retries=None) -> List[asyncpg.Record]:
    """Fetch all rows with retry logic and proper error handling"""
    async def _fetch(conn, *args):
        async with conn.transaction():
            return await conn.fetch(query, *args)
    
    return await execute_with_retry(_fetch, *args, retries=retries)

async def fetch_one(query: str, *args, retries=None) -> Optional[asyncpg.Record]:
    """Fetch single row with retry logic and proper error handling"""
    async def _fetch(conn, *args):
        async with conn.transaction():
            return await conn.fetchrow(query, *args)
    
    return await execute_with_retry(_fetch, *args, retries=retries)

# Add input validation for wallet addresses
def validate_column_name(table: str, column: str) -> bool:
    """Validate column name"""
    if table == 'notification_settings':
        return column in DB_CONFIG['ALLOWED_COLUMNS']['notification_settings']
    raise ValueError(f"Invalid table: {table}")

async def add_wallet(user_id: str, address: str, stake_address: str = None) -> bool:
    """Add a wallet with proper validation and error handling"""
    if not validate_address(address, 'wallet'):
        raise ValueError(f"Invalid wallet address format: {address}")
    
    if stake_address and not validate_address(stake_address, 'stake'):
        raise ValueError(f"Invalid stake address format: {stake_address}")
    
    try:
        queries = [
            (
                """
                INSERT INTO wallets (user_id, address, stake_address)
                VALUES ($1, $2, $3)
                ON CONFLICT (address) DO UPDATE
                SET user_id = $1, stake_address = $3
                RETURNING id
                """,
                (user_id, address, stake_address)
            ),
            (
                """
                INSERT INTO notification_settings (user_id)
                VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id,)
            )
        ]
        
        await execute_transaction(queries)
        logger.info(f"Successfully added wallet {address} for user {user_id}")
        return True
        
    except QueryError as e:
        logger.error(f"Failed to add wallet: {str(e)}")
        return False

async def execute_transaction(queries: List[tuple[str, tuple]], retries: int = None) -> None:
    """Execute multiple queries in a single transaction with retries
    
    Args:
        queries: List of (query, args) tuples to execute
        retries: Number of retry attempts
        
    Raises:
        QueryError: If transaction fails after all retries
    """
    async def _execute_transaction(conn, queries):
        async with conn.transaction():
            for query, args in queries:
                try:
                    await conn.execute(query, *args)
                except Exception as e:
                    logger.error(f"Error executing query in transaction: {e}")
                    raise QueryError(f"Transaction failed: {e}")
                    
    await execute_with_retry(_execute_transaction, queries, retries=retries)

async def update_notification_setting(user_id: str, setting: str, enabled: bool) -> bool:
    """Update a notification setting with proper validation
    
    Args:
        user_id: Discord user ID
        setting: Setting name to update
        enabled: New setting value
        
    Returns:
        bool: Success status
        
    Raises:
        ValueError: If setting name is invalid
    """
    if not validate_column_name('notification_settings', setting):
        raise ValueError(f"Invalid notification setting: {setting}")
        
    query = f"""
        UPDATE notification_settings 
        SET {setting} = $1 
        WHERE user_id = $2
    """
    
    try:
        await execute_transaction([(query, (enabled, user_id))])
        return True
    except QueryError as e:
        logger.error(f"Failed to update notification setting: {str(e)}")
        return False

async def add_transaction(wallet_id: int, tx_hash: str, metadata: dict = None) -> bool:
    """Add a transaction to the database with metadata
    
    Args:
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        metadata: Transaction metadata. Defaults to None.
        
    Returns:
        bool: Success status
    """
    try:
        async def _add_transaction(conn, wallet_id, tx_hash, metadata):
            async with conn.transaction():
                query = """
                    INSERT INTO transactions (wallet_id, tx_hash, metadata)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (wallet_id, tx_hash) 
                    DO UPDATE SET 
                        metadata = EXCLUDED.metadata,
                        created_at = CURRENT_TIMESTAMP
                    RETURNING id
                """
                result = await conn.fetchval(
                    query, 
                    wallet_id, 
                    tx_hash, 
                    json.dumps(metadata) if metadata else None
                )
                return bool(result)
        
        return await execute_with_retry(_add_transaction, wallet_id, tx_hash, metadata)
    except Exception as e:
        logger.error(f"Error adding transaction: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_transaction_metadata(wallet_id: int, tx_hash: str) -> Optional[dict]:
    """Get transaction metadata from the database
    
    Args:
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
    Returns:
        Optional[dict]: Transaction metadata or None if not found
    """
    try:
        async def _get_metadata(conn, wallet_id, tx_hash):
            query = """
                SELECT metadata
                FROM transactions
                WHERE wallet_id = $1 AND tx_hash = $2
            """
            result = await conn.fetchval(query, wallet_id, tx_hash)
            return json.loads(result) if result else None
        
        return await execute_with_retry(_get_metadata, wallet_id, tx_hash)
    except Exception as e:
        logger.error(f"Error getting transaction metadata: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def get_notification_settings(user_id: str):
    """Get user's notification settings
    
    Args:
        user_id: Discord user ID
        
    Returns:
        dict: Dictionary of notification settings
    """
    try:
        async def _get_settings(conn, user_id):
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
        
        return await execute_with_retry(_get_settings, user_id)
    except Exception as e:
        logger.error(f"Error getting notification settings: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def should_notify(user_id: str, notification_type: str) -> bool:
    """Check if a user should be notified about a specific event type"""
    try:
        async def _should_notify(conn, user_id, notification_type):
            result = await conn.fetchval(
                f"""
                SELECT {notification_type} 
                FROM notification_settings 
                WHERE user_id = $1
                """,
                user_id
            )
            return bool(result)
        
        return await execute_with_retry(_should_notify, user_id, notification_type)
    except Exception as e:
        logger.error(f"Error checking notification settings: {str(e)}")
        return True  # Default to notifying if there's an error

async def get_recent_transactions(address: str, hours: int = 1) -> List[asyncpg.Record]:
    """Get transactions in the last N hours
    
    Args:
        address: The wallet address
        hours: Number of hours to look back
        
    Returns:
        List[asyncpg.Record]: List of transactions
    """
    query = """
        SELECT t.tx_hash, t.metadata, t.created_at
        FROM transactions t
        JOIN wallets w ON w.id = t.wallet_id
        WHERE w.address = $1
          AND t.created_at > NOW() - $2 * INTERVAL '1 hour'
        ORDER BY t.created_at DESC
        LIMIT 100
    """
    try:
        return await fetch_all(query, address, hours)
    except QueryError as e:
        logger.error(f"Failed to get recent transactions: {str(e)}")
        return []

async def get_wallet_info(address: str) -> Dict[str, Any]:
    """Get comprehensive wallet information including stake address and delegation status
    
    Args:
        address: Wallet address
        
    Returns:
        Dict containing wallet info, stake address, and delegation status
    """
    query = """
        SELECT w.*, ns.ada_transactions, ns.token_changes, ns.nft_updates,
               ns.delegation_status, ns.policy_updates, ns.balance_alerts,
               s.stake_address, s.pool_id as delegation_pool
        FROM wallets w
        LEFT JOIN notification_settings ns ON w.user_id = ns.user_id
        LEFT JOIN stake_addresses s ON w.stake_address = s.stake_address
        WHERE w.address = $1
    """
    
    try:
        async with get_pool() as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, address)
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Error getting wallet info: {e}")
        raise DatabaseError(f"Failed to get wallet info: {e}")

async def get_user_wallets(user_id: str) -> List[Dict[str, Any]]:
    """Get all wallets for a user with optimized query
    
    Args:
        user_id: Discord user ID
        
    Returns:
        List of wallet records with all relevant information
    """
    query = """
        SELECT w.*, ns.ada_transactions, ns.token_changes, ns.nft_updates,
               ns.delegation_status, ns.policy_updates, ns.balance_alerts,
               s.stake_address, s.pool_id as delegation_pool
        FROM wallets w
        LEFT JOIN notification_settings ns ON w.user_id = ns.user_id
        LEFT JOIN stake_addresses s ON w.stake_address = s.stake_address
        WHERE w.user_id = $1
        ORDER BY w.created_at DESC
    """
    
    try:
        async with get_pool() as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, user_id)
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error getting user wallets: {e}")
        raise DatabaseError(f"Failed to get user wallets: {e}")

async def update_wallet_state(address: str, updates: Dict[str, Any]) -> bool:
    """Update multiple wallet attributes in a single transaction
    
    Args:
        address: Wallet address
        updates: Dictionary of updates to apply
            {
                'ada_balance': int,
                'token_balances': dict,
                'utxo_state': dict,
                'delegation_pool': str,
                'last_checked': datetime
            }
        
    Returns:
        bool: Success status
    """
    queries = []
    
    if 'ada_balance' in updates:
        queries.append((
            "UPDATE wallets SET ada_balance = $1 WHERE address = $2",
            (updates['ada_balance'], address)
        ))
        
    if 'token_balances' in updates:
        queries.append((
            "UPDATE wallets SET token_balances = $1 WHERE address = $2",
            (json.dumps(updates['token_balances']), address)
        ))
        
    if 'utxo_state' in updates:
        queries.append((
            "UPDATE wallets SET utxo_state = $1 WHERE address = $2",
            (json.dumps(updates['utxo_state']), address)
        ))
        
    if 'delegation_pool' in updates:
        queries.append((
            """
            INSERT INTO delegation_status (address, pool_id)
            SELECT address, $1 FROM wallets WHERE address = $2
            ON CONFLICT (address) DO UPDATE SET pool_id = $1
            """,
            (updates['delegation_pool'], address)
        ))
        
    if 'last_checked' in updates:
        queries.append((
            "UPDATE wallets SET last_updated = $1 WHERE address = $2",
            (updates['last_checked'], address)
        ))
        
    if not queries:
        return True
        
    try:
        await execute_transaction(queries)
        return True
    except QueryError as e:
        logger.error(f"Failed to update wallet state: {str(e)}")
        return False

async def check_ada_balance(address: str) -> tuple[bool, int]:
    """Check if ADA balance is below threshold
    
    Args:
        address: The wallet address
        
    Returns:
        tuple[bool, int]: (is_below_threshold, current_balance_ada)
    """
    try:
        async def _check_balance(conn, address):
            balance = await conn.fetchval(
                "SELECT ada_balance FROM wallets WHERE address = $1",
                address
            )
            
            if balance is None:
                return False, 0
                
            balance_ada = balance
            return balance_ada < 10, balance_ada
        
        return await execute_with_retry(_check_balance, address)
    except Exception as e:
        logger.error(f"Error checking ADA balance: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False, 0

async def update_ada_balance(address: str, balance: float) -> bool:
    """Update wallet's ADA balance
    
    Args:
        address: Wallet address
        balance: Current ADA balance
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_balance(conn, address, balance):
            await conn.execute(
                """
                UPDATE wallets 
                SET ada_balance = $2 
                WHERE address = $1
                """,
                address, balance
            )
            return True
        
        return await execute_with_retry(_update_balance, address, balance)
    except Exception as e:
        logger.error(f"Error updating ADA balance: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def update_token_balances(address: str, token_balances: dict) -> bool:
    """Update wallet's token balances
    
    Args:
        address: Wallet address
        token_balances: Dictionary of token_id -> amount
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_token_balances(conn, address, token_balances):
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
        
        return await execute_with_retry(_update_token_balances, address, token_balances)
    except Exception as e:
        logger.error(f"Error updating token balances: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_wallet_balance(address: str) -> int:
    """Get wallet's current ADA balance
    
    Args:
        address: Wallet address
        
    Returns:
        int: Current ADA balance in lovelace
    """
    try:
        async def _get_balance(conn, address):
            balance = await conn.fetchval(
                "SELECT ada_balance FROM wallets WHERE address = $1",
                address
            )
            return balance if balance is not None else 0
        
        return await execute_with_retry(_get_balance, address)
    except Exception as e:
        logger.error(f"Error getting wallet balance: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return 0

async def update_utxo_state(address: str, utxo_state: dict):
    """Update the UTxO state for a wallet address
    
    Args:
        address: Wallet address
        utxo_state: New UTxO state
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_utxo_state(conn, address, utxo_state):
            await conn.execute(
                """
                UPDATE wallets
                SET utxo_state = $1
                WHERE address = $2
                """,
                utxo_state, address
            )
            return True
        
        return await execute_with_retry(_update_utxo_state, address, utxo_state)
    except Exception as e:
        logger.error(f"Error updating UTxO state for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_stake_address(address: str):
    """Get cached stake address for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        Optional[str]: Stake address if cached, None otherwise
    """
    try:
        async def _get_stake_address(conn, address):
            row = await conn.fetchrow(
                """
                SELECT stake_address
                FROM wallets
                WHERE address = $1 AND stake_address IS NOT NULL
                """,
                address
            )
            return row['stake_address'] if row else None
        
        return await execute_with_retry(_get_stake_address, address)
    except Exception as e:
        logger.error(f"Error getting stake address for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def update_stake_address(address: str, stake_address: str):
    """Update stake address for a wallet
    
    Args:
        address: Wallet address
        stake_address: Stake address
    """
    try:
        async def _update_stake_address(conn, address, stake_address):
            await conn.execute(
                """
                UPDATE wallets
                SET stake_address = $2
                WHERE address = $1
                """,
                address,
                stake_address
            )
            return True
        
        return await execute_with_retry(_update_stake_address, address, stake_address)
    except Exception as e:
        logger.error(f"Error updating stake address for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def is_reward_processed(stake_address: str, epoch: int):
    """Check if a staking reward has been processed
    
    Args:
        stake_address: Stake address
        epoch: Epoch number
        
    Returns:
        bool: True if reward was processed
    """
    try:
        async def _is_reward_processed(conn, stake_address, epoch):
            row = await conn.fetchrow(
                """
                SELECT id
                FROM processed_rewards
                WHERE stake_address = $1 AND epoch = $2
                """,
                stake_address, epoch
            )
            return bool(row)
        
        return await execute_with_retry(_is_reward_processed, stake_address, epoch)
    except Exception as e:
        logger.error(f"Error checking reward for {stake_address[:20]} epoch {epoch}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def add_processed_reward(stake_address: str, epoch: int, amount: int):
    """Add a processed staking reward
    
    Args:
        stake_address: Stake address
        epoch: Epoch number
        amount: Reward amount in lovelace
        
    Returns:
        bool: Success status
    """
    try:
        async def _add_processed_reward(conn, stake_address, epoch, amount):
            await conn.execute(
                """
                INSERT INTO processed_rewards (stake_address, epoch, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (stake_address, epoch) DO NOTHING
                """,
                stake_address, epoch, amount
            )
            return True
        
        return await execute_with_retry(_add_processed_reward, stake_address, epoch, amount)
    except Exception as e:
        logger.error(f"Error adding processed reward: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_last_transactions(address: str) -> List[str]:
    """Retrieve the last set of processed transactions for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        List[str]: List of transaction hashes
    """
    try:
        async def _get_last_transactions(conn, address):
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
        
        return await execute_with_retry(_get_last_transactions, address)
    except Exception as e:
        logger.error(f"Error getting last transactions for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return []

async def get_utxo_state(address: str) -> Optional[dict]:
    """Get the UTxO state for a wallet address
    
    Args:
        address: Wallet address
        
    Returns:
        Optional[dict]: Dictionary containing UTxO state or None if not found
    """
    try:
        async def _get_utxo_state(conn, address):
            row = await conn.fetchrow(
                """
                SELECT utxo_state
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return row['utxo_state'] if row else None
        
        return await execute_with_retry(_get_utxo_state, address)
    except Exception as e:
        logger.error(f"Error getting UTxO state for {address[:20]}: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return None

async def get_wallet_for_user(user_id: str, address: str) -> Optional[dict]:
    """Get wallet details for a specific user and address
    
    Args:
        user_id: Discord user ID
        address: Wallet address
        
    Returns:
        Optional[dict]: Wallet record with all details or None if not found
    """
    try:
        async def _get_wallet_for_user(conn, user_id, address):
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
        
        return await execute_with_retry(_get_wallet_for_user, user_id, address)
    except Exception as e:
        logger.error(f"Error getting wallet for user {user_id} address {address}: {str(e)}")
        return None

async def is_token_change_processed(wallet_id: int, tx_hash: str) -> bool:
    """Check if a token change has been processed
    
    Args:
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
    Returns:
        bool: True if change was processed
    """
    try:
        async def _is_token_change_processed(conn, wallet_id, tx_hash):
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
        
        return await execute_with_retry(_is_token_change_processed, wallet_id, tx_hash)
    except Exception as e:
        logger.error(f"Error checking processed token change: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def add_processed_token_change(wallet_id: int, tx_hash: str) -> bool:
    """Add a processed token change
    
    Args:
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
    Returns:
        bool: Success status
    """
    try:
        async def _add_processed_token_change(conn, wallet_id, tx_hash):
            await conn.execute(
                """
                INSERT INTO transactions (wallet_id, tx_hash)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                wallet_id, tx_hash
            )
            return True
        
        return await execute_with_retry(_add_processed_token_change, wallet_id, tx_hash)
    except Exception as e:
        logger.error(f"Error adding processed token change: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return False

async def get_new_tokens(address: str) -> List[str]:
    """Get new tokens added to a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        List[str]: List of new token IDs
    """
    try:
        async def _get_new_tokens(conn, address):
            old_state = await get_utxo_state(address)
            if not old_state:
                return []
                
            old_tokens = set(old_state.get('tokens', {}).keys())
            current_state = await get_utxo_state(address)
            if not current_state:
                return []
                
            current_tokens = set(current_state.get('tokens', {}).keys())
            return list(current_tokens - old_tokens)
        
        return await execute_with_retry(_get_new_tokens, address)
    except Exception as e:
        logger.error(f"Error getting new tokens: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return []

async def get_removed_nfts(address: str) -> List[str]:
    """Get NFTs removed from a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        List[str]: List of removed NFT IDs
    """
    try:
        async def _get_removed_nfts(conn, address):
            old_state = await get_utxo_state(address)
            if not old_state:
                return []
                
            old_nfts = set(old_state.get('nfts', []))
            current_state = await get_utxo_state(address)
            if not current_state:
                return []
                
            current_nfts = set(current_state.get('nfts', []))
            return list(old_nfts - current_nfts)
        
        return await execute_with_retry(_get_removed_nfts, address)
    except Exception as e:
        logger.error(f"Error getting removed NFTs: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        return []

async def get_yummi_warning_count(wallet_id: int) -> int:
    """Get the number of YUMMI warnings for a wallet
    
    Args:
        wallet_id: Wallet ID
        
    Returns:
        int: Number of warnings (0 if no warnings)
    """
    try:
        async def _get_yummi_warning_count(conn, wallet_id):
            result = await conn.fetchval(
                """
                SELECT warning_count 
                FROM yummi_warnings 
                WHERE wallet_id = $1
                """,
                wallet_id
            )
            return result or 0
        
        return await execute_with_retry(_get_yummi_warning_count, wallet_id)
    except Exception as e:
        logger.error(f"Error getting YUMMI warning count: {e}")
        return 0

async def increment_yummi_warning(wallet_id: int) -> int:
    """Increment the YUMMI warning count for a wallet
    
    Args:
        wallet_id: Wallet ID
        
    Returns:
        int: New warning count
    """
    try:
        async def _increment_yummi_warning(conn, wallet_id):
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
        
        return await execute_with_retry(_increment_yummi_warning, wallet_id)
    except Exception as e:
        logger.error(f"Error incrementing YUMMI warning: {e}")
        return 0

async def reset_yummi_warning(wallet_id: int) -> bool:
    """Reset the YUMMI warning count for a wallet
    
    Args:
        wallet_id: Wallet ID
        
    Returns:
        bool: Success status
    """
    try:
        async def _reset_yummi_warning(conn, wallet_id):
            await conn.execute(
                """
                DELETE FROM yummi_warnings
                WHERE wallet_id = $1
                """,
                wallet_id
            )
            return True
        
        return await execute_with_retry(_reset_yummi_warning, wallet_id)
    except Exception as e:
        logger.error(f"Error resetting YUMMI warning: {e}")
        return False

async def get_delegation_status(address: str):
    """Get the current delegation status for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        str: Pool ID or None if not delegated
    """
    try:
        async def _get_delegation_status(conn, address):
            result = await conn.fetchval(
                """
                SELECT pool_id
                FROM delegation_status
                WHERE address = $1
                """,
                address
            )
            return result
        
        return await execute_with_retry(_get_delegation_status, address)
    except Exception as e:
        logger.error(f"Error getting delegation status: {e}")
        return None

async def update_delegation_status(address: str, pool_id: str):
    """Update the delegation status for a wallet
    
    Args:
        address: Wallet address
        pool_id: Pool ID
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_delegation_status(conn, address, pool_id):
            await conn.execute(
                """
                INSERT INTO delegation_status (address, pool_id)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET pool_id = $2
                """,
                address, pool_id
            )
            return True
        
        return await execute_with_retry(_update_delegation_status, address, pool_id)
    except Exception as e:
        logger.error(f"Error updating delegation status: {e}")
        return False

async def get_policy_expiry(policy_id: str):
    """Get the expiry time for a policy
    
    Args:
        policy_id: Policy ID
        
    Returns:
        int: Slot number when policy expires or None
    """
    try:
        async def _get_policy_expiry(conn, policy_id):
            result = await conn.fetchval(
                """
                SELECT expiry_slot
                FROM policy_expiry
                WHERE policy_id = $1
                """,
                policy_id
            )
            return result
        
        return await execute_with_retry(_get_policy_expiry, policy_id)
    except Exception as e:
        logger.error(f"Error getting policy expiry: {e}")
        return None

async def update_policy_expiry(policy_id: str, expiry_slot: int):
    """Update or insert policy expiry information
    
    Args:
        policy_id: Policy ID
        expiry_slot: Slot number when policy expires
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_policy_expiry(conn, policy_id, expiry_slot):
            await conn.execute(
                """
                INSERT INTO policy_expiry (policy_id, expiry_slot)
                VALUES ($1, $2)
                ON CONFLICT (policy_id) DO UPDATE
                SET expiry_slot = $2
                """,
                policy_id, expiry_slot
            )
            return True
        
        return await execute_with_retry(_update_policy_expiry, policy_id, expiry_slot)
    except Exception as e:
        logger.error(f"Error updating policy expiry: {e}")
        return False

async def get_dapp_interactions(address: str) -> str:
    """Get the last processed DApp interaction for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        str: Last processed tx hash or None
    """
    try:
        async def _get_dapp_interactions(conn, address):
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
        
        return await execute_with_retry(_get_dapp_interactions, address)
    except Exception as e:
        logger.error(f"Error getting DApp interactions for {address[:20]}: {e}")
        return None

async def update_dapp_interaction(address: str, tx_hash: str) -> bool:
    """Update the last processed DApp interaction
    
    Args:
        address: Wallet address
        tx_hash: Transaction hash
        
    Returns:
        bool: Success status
    """
    try:
        async def _update_dapp_interaction(conn, address, tx_hash):
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
        
        return await execute_with_retry(_update_dapp_interaction, address, tx_hash)
    except Exception as e:
        logger.error(f"Error updating DApp interaction for {address[:20]}: {e}")
        return False

async def get_last_dapp_tx(address: str) -> Optional[str]:
    """Get the last processed DApp transaction for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        Optional[str]: Transaction hash or None if not found
    """
    try:
        async def _get_last_dapp_tx(conn, address):
            query = """
                SELECT tx_hash
                FROM dapp_interactions
                WHERE address = $1
                ORDER BY created_at DESC
                LIMIT 1
            """
            result = await conn.fetchval(query, address)
            return result
        
        return await execute_with_retry(_get_last_dapp_tx, address)
    except Exception as e:
        logger.error(f"Error getting last DApp transaction: {e}")
        return None

async def update_last_dapp_tx(address: str, tx_hash: str) -> bool:
    """Update the last processed DApp transaction for a wallet
    
    Args:
        address: Wallet address
        tx_hash: Transaction hash
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        async def _update_last_dapp_tx(conn, address, tx_hash):
            query = """
                INSERT INTO dapp_interactions (address, tx_hash)
                VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE
                SET tx_hash = $2
            """
            await conn.execute(query, address, tx_hash)
            return True
        
        return await execute_with_retry(_update_last_dapp_tx, address, tx_hash)
    except Exception as e:
        logger.error(f"Error updating last DApp transaction: {e}")
        return False

async def add_failed_transaction(wallet_id: int, tx_hash: str, error_type: str, error_details: dict) -> bool:
    """Add a failed transaction to the database"""
    try:
        async def _add_failed_transaction(conn, wallet_id, tx_hash, error_type, error_details):
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
        
        return await execute_with_retry(_add_failed_transaction, wallet_id, tx_hash, error_type, error_details)
    except Exception as e:
        logger.error(f"Error adding failed transaction: {e}")
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
        async def _add_asset_history(conn, wallet_id, asset_id, tx_hash, action, quantity, metadata):
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
        
        return await execute_with_retry(_add_asset_history, wallet_id, asset_id, tx_hash, action, quantity, metadata)
    except Exception as e:
        logger.error(f"Error adding asset history: {e}")
        return False

async def initialize_notification_settings(user_id: str):
    """Initialize default notification settings for a new user
    
    Args:
        user_id: Discord user ID
    """
    try:
        async def _initialize_notification_settings(conn, user_id):
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
                
        await execute_with_retry(_initialize_notification_settings, user_id)
    except Exception as e:
        logger.error(f"Error initializing notification settings: {e}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")

async def get_user_id_for_stake_address(stake_address: str) -> Optional[str]:
    """Get the user ID associated with a stake address
    
    Args:
        stake_address: Stake address
        
    Returns:
        Optional[str]: Discord user ID or None if not found
    """
    try:
        async def _get_user_id_for_stake_address(conn, stake_address):
            query = """
                SELECT DISTINCT user_id 
                FROM wallets 
                WHERE stake_address = $1
            """
            result = await conn.fetchval(query, stake_address)
            return result
        
        return await execute_with_retry(_get_user_id_for_stake_address, stake_address)
    except Exception as e:
        logger.error(f"Error getting user ID for stake address: {e}")
        return None

async def get_last_policy_check(address: str):
    """Get the last time policy expiry was checked
    
    Args:
        address: Wallet address
        
    Returns:
        Optional[datetime]: Last check time or None if never checked
    """
    try:
        async def _get_last_policy_check(conn, address):
            row = await conn.fetchrow(
                """
                SELECT last_policy_check
                FROM wallets
                WHERE address = $1
                """,
                address
            )
            return row['last_policy_check'] if row else None
        
        return await execute_with_retry(_get_last_policy_check, address)
    except Exception as e:
        logger.error(f"Error getting last policy check: {e}")
        return None

async def update_last_policy_check(address: str):
    """Update the last policy check time
    
    Args:
        address: Wallet address
    """
    try:
        async def _update_last_policy_check(conn, address):
            await conn.execute(
                """
                UPDATE wallets
                SET last_policy_check = NOW()
                WHERE address = $1
                """,
                address
            )
        
        await execute_with_retry(_update_last_policy_check, address)
    except Exception as e:
        logger.error(f"Error updating last policy check: {e}")

async def get_monitoring_since(address: str) -> datetime:
    """Get when monitoring started for a wallet
    
    Args:
        address: Wallet address
        
    Returns:
        datetime: When monitoring started
    """
    try:
        async def _get_monitoring_since(conn, address):
            return await conn.fetchval(
                """
                SELECT monitoring_since
                FROM wallets
                WHERE address = $1
                """,
                address
            )
        
        return await execute_with_retry(_get_monitoring_since, address)
    except Exception as e:
        logger.error(f"Error getting monitoring since: {e}")
        return None

async def get_all_monitored_addresses(pool):
    """Get all monitored addresses and stake addresses
    
    Args:
        pool: Database connection pool
        
    Returns:
        List[str]: List of monitored addresses
    """
    try:
        query = """
            SELECT DISTINCT address 
            FROM wallets;
        """
        
        result = await pool.fetch(query)
        return [row['address'] for row in result] if result else []
        
    except Exception as e:
        logger.error(f"Error getting monitored addresses: {e}")
        return []

async def get_addresses_for_stake(stake_address: str) -> list[str]:
    """Get all addresses for a stake address"""
    try:
        async def _get_addresses_for_stake(conn, stake_address):
            async with conn.execute(
                """
                SELECT address
                FROM wallets
                WHERE stake_address = $1
                """,
                stake_address
            ) as cursor:
                return [row[0] async for row in cursor]
        
        return await execute_with_retry(_get_addresses_for_stake, stake_address)
    except Exception as e:
        logger.error(f"Error getting addresses for stake: {e}")
        return []

async def update_pool_for_stake(stake_address: str, pool_id: str):
    """Update the pool ID for a stake address"""
    try:
        async def _update_pool_for_stake(conn, stake_address, pool_id):
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
        
        return await execute_with_retry(_update_pool_for_stake, stake_address, pool_id)
    except Exception as e:
        logger.error(f"Error updating pool for stake address: {e}")
        return False

async def update_stake_pool(stake_address: str, pool_id: str) -> bool:
    """Update stake pool for an address"""
    try:
        async def _update_stake_pool(conn, stake_address, pool_id):
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
        
        return await execute_with_retry(_update_stake_pool, stake_address, pool_id)
    except Exception as e:
        logger.error(f"Error updating stake pool: {e}")
        return False

async def remove_wallet_for_user(user_id: str, address: str):
    """Remove a wallet from monitoring
    
    Args:
        user_id: Discord user ID
        address: Wallet address to remove
        
    Returns:
        bool: Success status
    """
    try:
        async def _remove_wallet_for_user(conn, user_id, address):
            await conn.execute(
                """
                DELETE FROM wallets
                WHERE user_id = $1 AND address = $2
                """,
                user_id, address
            )
            return True
        
        return await execute_with_retry(_remove_wallet_for_user, user_id, address)
    except Exception as e:
        logger.error(f"Failed to remove wallet: {e}")
        return False

async def execute_query(query: str, *args) -> None:
    """Execute a database query with retry logic"""
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.execute(query, *args)
        except asyncpg.exceptions.ConnectionDoesNotExistError:
            # Connection was closed, retry
            global _pool
            _pool = None  # Force pool recreation
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            raise
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise

async def execute_many(query: str, args_list: list) -> None:
    """Execute a database query with multiple sets of parameters
    
    Args:
        query: SQL query to execute
        args_list: List of parameter sets
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, args_list)
            logger.debug(f"Executed batch query: {query}")

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

def init_db_sync():
    """Initialize database synchronously"""
    try:
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Get database URL
            database_url = get_database_url()
            
            # Create SSL context
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # Create and set global pool
            global _pool
            _pool = loop.run_until_complete(
                asyncpg.create_pool(
                    database_url,
                    min_size=5,
                    max_size=20,
                    command_timeout=60,
                    ssl=ssl_context
                )
            )
            
            # Initialize database
            loop.run_until_complete(init_db())
            logger.info("Database initialized successfully")
            
        finally:
            # Close the loop
            loop.close()
            
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

class DatabaseError(Exception):
    """Base class for database exceptions"""
    pass

class ConnectionError(DatabaseError):
    """Database connection error"""
    pass

class QueryError(DatabaseError):
    """Database query error"""
    pass

async def get_db_version(conn) -> int:
    """Get current database version"""
    try:
        version = await conn.fetchval(
            "SELECT version FROM db_version ORDER BY last_updated DESC LIMIT 1"
        )
        return version or 0
    except Exception as e:
        logger.error(f"Error getting database version: {e}")
        return 0

async def set_db_version(conn, version: int):
    """Set database version"""
    try:
        await conn.execute(
            """
            INSERT INTO db_version (version, created_at, last_updated)
            VALUES ($1, NOW(), NOW())
            """,
            version
        )
        logger.info(f"Database version set to {version}")
    except Exception as e:
        logger.error(f"Error setting database version: {e}")
        raise

# Current database version
CURRENT_VERSION = 1

# Database migrations
MIGRATIONS = {
    # Version 1: Initial schema setup
    1: [
        # Step 1: Create base tables
        """
        -- Create wallets table
        CREATE TABLE IF NOT EXISTS wallets (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            address TEXT NOT NULL,
            stake_address TEXT,
            ada_balance NUMERIC DEFAULT 0,
            monitoring_since TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            last_policy_check TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            UNIQUE(user_id, address)
        );
        """,
        
        """
        -- Create notification_settings table
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
        """,
        
        """
        -- Create transactions table
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL,
            wallet_id INTEGER REFERENCES wallets(id),
            tx_hash TEXT NOT NULL,
            metadata JSONB,
            archived BOOLEAN DEFAULT FALSE,
            archived_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        ) PARTITION BY RANGE (created_at);
        """,
        
        """
        -- Create partitions for transactions
        DO $$
        BEGIN
            FOR y IN 2025..2025 LOOP
                FOR m IN 1..12 LOOP
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS transactions_%s_%s PARTITION OF transactions
                        FOR VALUES FROM (%L) TO (%L)',
                        y, LPAD(m::text, 2, '0'),
                        format('%s-%s-01 00:00:00+00', y, LPAD(m::text, 2, '0')),
                        format('%s-%s-01 00:00:00+00', 
                            CASE WHEN m = 12 THEN y + 1 ELSE y END,
                            CASE WHEN m = 12 THEN '01' ELSE LPAD((m + 1)::text, 2, '0') END
                        )
                    );
                END LOOP;
            END LOOP;
        END $$;
        """,
        
        """
        -- Create failed_transactions table
        CREATE TABLE IF NOT EXISTS failed_transactions (
            id SERIAL PRIMARY KEY,
            wallet_id INTEGER REFERENCES wallets(id),
            tx_hash TEXT NOT NULL,
            error_type TEXT,
            error_details JSONB,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create asset_history table
        CREATE TABLE IF NOT EXISTS asset_history (
            id SERIAL PRIMARY KEY,
            wallet_id INTEGER REFERENCES wallets(id),
            asset_id TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            asset_name TEXT,
            quantity NUMERIC,
            action TEXT,
            tx_hash TEXT,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create token_balances table
        CREATE TABLE IF NOT EXISTS token_balances (
            id SERIAL PRIMARY KEY,
            address TEXT NOT NULL,
            token_id TEXT NOT NULL,
            balance NUMERIC DEFAULT 0,
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create delegation_status table
        CREATE TABLE IF NOT EXISTS delegation_status (
            id SERIAL PRIMARY KEY,
            address TEXT NOT NULL,
            pool_id TEXT NOT NULL,
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create processed_rewards table
        CREATE TABLE IF NOT EXISTS processed_rewards (
            id SERIAL PRIMARY KEY,
            stake_address TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            amount NUMERIC NOT NULL,
            processed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create policy_expiry table
        CREATE TABLE IF NOT EXISTS policy_expiry (
            id SERIAL PRIMARY KEY,
            policy_id TEXT NOT NULL,
            expiry_slot BIGINT NOT NULL,
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create dapp_interactions table
        CREATE TABLE IF NOT EXISTS dapp_interactions (
            id SERIAL PRIMARY KEY,
            address TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        
        """
        -- Create stake_addresses table
        CREATE TABLE IF NOT EXISTS stake_addresses (
            stake_address TEXT PRIMARY KEY,
            last_pool_id TEXT,
            last_checked TIMESTAMP WITHOUT TIME ZONE
        );
        """,
        
        # Step 2: Create indices for wallets
        """
        CREATE INDEX IF NOT EXISTS idx_wallets_user_id ON wallets(user_id);
        CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address);
        CREATE INDEX IF NOT EXISTS idx_wallets_stake_address ON wallets(stake_address);
        CREATE INDEX IF NOT EXISTS idx_wallets_last_updated ON wallets(last_updated DESC);
        """,
        
        # Step 3: Create indices for notification_settings
        """
        CREATE INDEX IF NOT EXISTS idx_notification_settings_user_id ON notification_settings(user_id);
        """,
        
        # Step 4: Create indices for transactions (on parent table)
        """
        CREATE INDEX IF NOT EXISTS idx_transactions_wallet_tx ON transactions(wallet_id, tx_hash);
        CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at DESC);
        """,
        
        # Step 5: Create indices for failed_transactions
        """
        CREATE INDEX IF NOT EXISTS idx_failed_transactions_wallet ON failed_transactions(wallet_id);
        CREATE INDEX IF NOT EXISTS idx_failed_transactions_created ON failed_transactions(created_at DESC);
        """,
        
        # Step 6: Create indices for asset_history
        """
        CREATE INDEX IF NOT EXISTS idx_asset_history_wallet ON asset_history(wallet_id);
        CREATE INDEX IF NOT EXISTS idx_asset_history_asset ON asset_history(asset_id);
        CREATE INDEX IF NOT EXISTS idx_asset_history_created ON asset_history(created_at DESC);
        """,
        
        # Token balances indices
        """
        CREATE INDEX IF NOT EXISTS idx_token_balances_address ON token_balances(address);
        CREATE INDEX IF NOT EXISTS idx_token_balances_token ON token_balances(token_id);
        """,
        
        # Delegation status indices
        """
        CREATE INDEX IF NOT EXISTS idx_delegation_status_address ON delegation_status(address);
        """,
        
        # Processed rewards indices
        """
        CREATE INDEX IF NOT EXISTS idx_processed_rewards_stake_address ON processed_rewards(stake_address);
        CREATE INDEX IF NOT EXISTS idx_processed_rewards_epoch ON processed_rewards(epoch);
        """,
        
        # Policy expiry indices
        """
        CREATE INDEX IF NOT EXISTS idx_policy_expiry_policy_id ON policy_expiry(policy_id);
        """,
        
        # DApp interactions indices
        """
        CREATE INDEX IF NOT EXISTS idx_dapp_interactions_address ON dapp_interactions(address);
        """,
        
        # Stake addresses indices
        """
        CREATE INDEX IF NOT EXISTS idx_stake_addresses_stake_address ON stake_addresses(stake_address);
        """,
    ]
}

async def migrate_database(conn) -> bool:
    """Run database migrations with proper error handling"""
    try:
        # Get current version
        current = await get_db_version(conn)
        
        if current >= CURRENT_VERSION:
            logger.info(f"Database is already at version {current}")
            return True
            
        # Run migrations in order
        for version in range(current + 1, CURRENT_VERSION + 1):
            migration_steps = MIGRATIONS.get(version)
            if not migration_steps:
                logger.warning(f"No migration found for version {version}")
                continue
                
            try:
                logger.info(f"Applying migration {version}")
                
                # Execute each step in the migration
                for step in migration_steps:
                    try:
                        async with conn.transaction():
                            await conn.execute(step)
                    except Exception as e:
                        logger.error(f"Error executing migration step: {e}")
                        if hasattr(e, '__dict__'):
                            logger.error(f"Error details: {e.__dict__}")
                        # Continue with next step
                        continue
                
                async with conn.transaction():
                    # Record successful migration
                    await conn.execute(
                        """
                        INSERT INTO migration_history (version, applied_at, success)
                        VALUES ($1, NOW(), TRUE)
                        """,
                        version
                    )
                    
                    # Update db version
                    await conn.execute(
                        """
                        INSERT INTO db_version (version, created_at, last_updated)
                        VALUES ($1, NOW(), NOW())
                        ON CONFLICT (id) DO UPDATE
                        SET version = EXCLUDED.version,
                            last_updated = NOW()
                        """,
                        version
                    )
                    
                    logger.info(f"Successfully applied migration {version}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error applying migration {version}: {error_msg}")
                
                try:
                    # Record failed migration
                    await conn.execute(
                        """
                        INSERT INTO migration_history (version, applied_at, success, error)
                        VALUES ($1, NOW(), FALSE, $2)
                        """,
                        version, error_msg
                    )
                except Exception as inner_e:
                    logger.error(f"Error recording migration failure: {inner_e}")
                
                raise DatabaseError(f"Migration {version} failed: {error_msg}")
                
        logger.info("Database migrations completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database migration failed: {e}")
        raise DatabaseError(f"Failed to migrate database: {e}")
