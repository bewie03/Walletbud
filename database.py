import os
import json
import logging
import asyncio
import asyncpg
from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Database configuration constants
DB_CONFIG = {
    'MIN_POOL_SIZE': int(os.getenv('DB_MIN_POOL_SIZE', '2')),
    'MAX_POOL_SIZE': int(os.getenv('DB_MAX_POOL_SIZE', '10')),
    'MAX_QUERIES_PER_CONN': int(os.getenv('DB_MAX_QUERIES', '50000')),
    'COMMAND_TIMEOUT': int(os.getenv('DB_COMMAND_TIMEOUT', '60')),
    'TRANSACTION_TIMEOUT': int(os.getenv('DB_TRANSACTION_TIMEOUT', '60')),
    'RETRY_ATTEMPTS': int(os.getenv('DB_RETRY_ATTEMPTS', '3')),
    'RETRY_DELAY': int(os.getenv('DB_RETRY_DELAY', '1')),
    'ALLOWED_COLUMNS': {
        'notification_settings': {'ada_transactions', 'token_changes', 'nft_updates', 
                                'delegation_status', 'policy_updates', 'balance_alerts'}
    }
}

# Global connection pool with proper locking
_pool = None
_pool_lock = asyncio.Lock()
_pool_init_error = None

class DatabaseError(Exception):
    """Base class for database exceptions"""
    pass

class ConnectionError(DatabaseError):
    """Database connection error"""
    pass

class QueryError(DatabaseError):
    """Database query error"""
    pass

async def init_db():
    """Initialize database connection pool with proper error handling"""
    global _pool, _pool_init_error
    
    if _pool_init_error:
        logger.warning("Previous pool initialization failed, resetting error state")
        _pool_init_error = None
    
    async with _pool_lock:
        if _pool is not None:
            logger.warning("Database pool already initialized")
            return _pool
            
        try:
            # Determine SSL mode based on URL
            ssl_required = 'amazonaws.com' in DATABASE_URL or 'herokuapp.com' in DATABASE_URL
            ssl_mode = 'require' if ssl_required else None
            
            # Create connection pool with configurable settings
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=DB_CONFIG['MIN_POOL_SIZE'],
                max_size=DB_CONFIG['MAX_POOL_SIZE'],
                command_timeout=DB_CONFIG['COMMAND_TIMEOUT'],
                ssl=ssl_mode,
                max_cached_statement_lifetime=600,
                max_queries=DB_CONFIG['MAX_QUERIES_PER_CONN'],
                server_settings={
                    'application_name': 'WalletBud',
                    'statement_timeout': f"{DB_CONFIG['COMMAND_TIMEOUT']}000",
                    'idle_in_transaction_session_timeout': f"{DB_CONFIG['TRANSACTION_TIMEOUT']}000",
                    'lock_timeout': '10000'  # 10 second lock timeout
                }
            )
            
            # Test connection
            async with _pool.acquire() as conn:
                await conn.execute('SELECT 1')
                
            logger.info(
                f"Database pool initialized (size: {DB_CONFIG['MIN_POOL_SIZE']}-{DB_CONFIG['MAX_POOL_SIZE']})"
            )
            return _pool
            
        except Exception as e:
            _pool_init_error = str(e)
            logger.error(f"Failed to initialize database pool: {str(e)}")
            raise ConnectionError(f"Database initialization failed: {str(e)}")

async def get_pool():
    """Get database connection pool with initialization check"""
    global _pool
    if _pool is None:
        # Create connection pool with configurable settings
        try:
            # Determine SSL mode based on URL
            ssl_required = 'amazonaws.com' in DATABASE_URL or 'herokuapp.com' in DATABASE_URL
            ssl_mode = 'require' if ssl_required else None
            
            # Create connection pool with configurable settings
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=DB_CONFIG['MIN_POOL_SIZE'],
                max_size=DB_CONFIG['MAX_POOL_SIZE'],
                command_timeout=DB_CONFIG['COMMAND_TIMEOUT'],
                ssl=ssl_mode,
                max_cached_statement_lifetime=600,
                max_queries=DB_CONFIG['MAX_QUERIES_PER_CONN'],
                server_settings={
                    'application_name': 'WalletBud',
                    'statement_timeout': f"{DB_CONFIG['COMMAND_TIMEOUT']}000",
                    'idle_in_transaction_session_timeout': f"{DB_CONFIG['TRANSACTION_TIMEOUT']}000",
                    'lock_timeout': '10000'  # 10 second lock timeout
                }
            )
            
            # Test connection
            async with _pool.acquire() as conn:
                await conn.execute('SELECT 1')
                
            logger.info(
                f"Database pool initialized (size: {DB_CONFIG['MIN_POOL_SIZE']}-{DB_CONFIG['MAX_POOL_SIZE']})"
            )
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {str(e)}")
            _pool = None
            raise ConnectionError(f"Database initialization failed: {str(e)}")
            
    return _pool

async def fetch_all(query: str, *args, retries: int = None) -> List[asyncpg.Record]:
    """Fetch all rows from a database query with retries
    
    Args:
        query: SQL query to execute
        *args: Query parameters
        retries: Number of retry attempts
        
    Returns:
        List[asyncpg.Record]: Query results
        
    Raises:
        QueryError: If query fails after all retries
    """
    retries = retries if retries is not None else DB_CONFIG['RETRY_ATTEMPTS']
    last_error = None
    
    for attempt in range(retries):
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetch(query, *args)
                    
        except asyncpg.InterfaceError:
            # Connection is broken, force pool reinitialization
            global _pool
            _pool = None
            continue
            
        except asyncpg.PostgresError as e:
            last_error = e
            if attempt < retries - 1:
                delay = DB_CONFIG['RETRY_DELAY'] * (2 ** attempt)
                logger.warning(f"Query failed (attempt {attempt + 1}/{retries}), retrying in {delay}s: {str(e)}")
                await asyncio.sleep(delay)
            
        except Exception as e:
            raise QueryError(f"Unexpected error executing query: {str(e)}")
            
    raise QueryError(f"Query failed after {retries} attempts: {str(last_error)}")

async def fetch_one(query: str, *args, retries: int = None) -> Optional[asyncpg.Record]:
    """Fetch a single row with retries
    
    Args:
        query: SQL query to execute
        *args: Query parameters
        retries: Number of retry attempts
        
    Returns:
        Optional[asyncpg.Record]: Query result or None if not found
        
    Raises:
        QueryError: If query fails after all retries
    """
    retries = retries if retries is not None else DB_CONFIG['RETRY_ATTEMPTS']
    last_error = None
    
    for attempt in range(retries):
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchrow(query, *args)
                    
        except asyncpg.InterfaceError:
            # Connection is broken, force pool reinitialization
            global _pool
            _pool = None
            continue
            
        except asyncpg.PostgresError as e:
            last_error = e
            if attempt < retries - 1:
                delay = DB_CONFIG['RETRY_DELAY'] * (2 ** attempt)
                logger.warning(f"Query failed (attempt {attempt + 1}/{retries}), retrying in {delay}s: {str(e)}")
                await asyncio.sleep(delay)
            
        except Exception as e:
            raise QueryError(f"Unexpected error executing query: {str(e)}")
            
    raise QueryError(f"Query failed after {retries} attempts: {str(last_error)}")

async def execute_transaction(queries: List[tuple[str, tuple]], retries: int = None) -> None:
    """Execute multiple queries in a single transaction with retries
    
    Args:
        queries: List of (query, args) tuples to execute
        retries: Number of retry attempts
        
    Raises:
        QueryError: If transaction fails after all retries
    """
    pool = await get_pool()
    retry_count = retries if retries is not None else DB_CONFIG['RETRY_ATTEMPTS']
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            for query, args in queries:
                try:
                    await conn.execute(query, *args)
                except Exception as e:
                    logger.error(f"Error executing query in transaction: {e}")
                    if retry_count > 0:
                        await asyncio.sleep(DB_CONFIG['RETRY_DELAY'])
                        return await execute_transaction(queries, retry_count - 1)
                    raise QueryError(f"Transaction failed after {DB_CONFIG['RETRY_ATTEMPTS']} retries: {e}")

async def add_wallet(user_id: str, address: str, stake_address: str = None) -> bool:
    """Add a wallet with proper validation and error handling
    
    Args:
        user_id: Discord user ID
        address: Wallet address
        stake_address: Optional stake address
        
    Returns:
        bool: Success status
        
    Raises:
        ValueError: If input validation fails
    """
    # Validate inputs
    if not user_id or not address:
        raise ValueError("user_id and address are required")
        
    if not isinstance(user_id, str) or not isinstance(address, str):
        raise ValueError("user_id and address must be strings")
        
    if stake_address and not isinstance(stake_address, str):
        raise ValueError("stake_address must be a string if provided")
        
    try:
        # Check if wallet already exists
        existing = await fetch_one(
            "SELECT id FROM wallets WHERE address = $1",
            address
        )
        if existing:
            logger.warning(f"Wallet {address} already exists")
            return False
            
        # Add wallet and initialize notification settings in transaction
        queries = [
            (
                """
                INSERT INTO wallets (user_id, address, stake_address)
                VALUES ($1, $2, $3)
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
DROP TABLE IF EXISTS transactions_by_month CASCADE;

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
    id SERIAL,
    wallet_id INTEGER REFERENCES wallets(id),
    tx_hash TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    archived BOOLEAN DEFAULT FALSE,
    archived_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY(id, created_at),
    UNIQUE(wallet_id, tx_hash, created_at)
) PARTITION BY RANGE (created_at);

DO $$
BEGIN
    FOR i IN 0..11 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS transactions_%s_%s PARTITION OF transactions 
            FOR VALUES FROM (%L) TO (%L)',
            to_char(CURRENT_DATE + (interval '1 month' * i), 'YYYY'),
            to_char(CURRENT_DATE + (interval '1 month' * i), 'MM'),
            CURRENT_DATE + (interval '1 month' * i),
            CURRENT_DATE + (interval '1 month' * (i + 1))
        );
    END LOOP;
END $$;

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

async def check_database_schema(pool) -> None:
    """Verify database schema including tables, columns, constraints and indices
    
    Raises:
        DatabaseError: If schema verification fails
    """
    try:
        async with pool.acquire() as conn:
            # Check tables
            tables = await conn.fetch("""
                SELECT table_name, column_name, data_type, 
                       is_nullable, column_default,
                       character_maximum_length
                FROM information_schema.columns 
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
            """)
            
            # Check constraints
            constraints = await conn.fetch("""
                SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
                       kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'public'
            """)
            
            # Check indices
            indices = await conn.fetch("""
                SELECT schemaname, tablename, indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
            """)
            
            # Validate expected schema
            expected_tables = {
                'wallets': {
                    'columns': {'id', 'user_id', 'address', 'stake_address', 'created_at'},
                    'constraints': {'wallets_pkey', 'wallets_address_key'},
                    'indices': {'idx_wallets_user_id', 'idx_wallets_address'}
                },
                'notification_settings': {
                    'columns': {'user_id', 'ada_transactions', 'token_transfers', 
                              'nft_updates', 'delegation_status', 'policy_updates'},
                    'constraints': {'notification_settings_pkey'},
                    'indices': {'idx_notification_settings_user_id'}
                },
                # Add other tables here
            }
            
            # Verify tables and columns
            found_tables = {t['table_name'] for t in tables}
            for table, expected in expected_tables.items():
                if table not in found_tables:
                    raise DatabaseError(f"Missing required table: {table}")
                    
                found_columns = {t['column_name'] for t in tables if t['table_name'] == table}
                missing_columns = expected['columns'] - found_columns
                if missing_columns:
                    raise DatabaseError(f"Missing columns in {table}: {missing_columns}")
                    
            # Verify constraints
            for table, expected in expected_tables.items():
                found_constraints = {c['constraint_name'] for c in constraints 
                                  if c['table_name'] == table}
                missing_constraints = expected['constraints'] - found_constraints
                if missing_constraints:
                    raise DatabaseError(f"Missing constraints in {table}: {missing_constraints}")
                    
            # Verify indices
            for table, expected in expected_tables.items():
                found_indices = {i['indexname'] for i in indices if i['tablename'] == table}
                missing_indices = expected['indices'] - found_indices
                if missing_indices:
                    raise DatabaseError(f"Missing indices in {table}: {missing_indices}")
                    
            logger.info("Database schema verification completed successfully")
            
    except asyncpg.PostgresError as e:
        raise DatabaseError(f"Schema verification failed: {str(e)}")
        
    except Exception as e:
        raise DatabaseError(f"Unexpected error during schema verification: {str(e)}")

async def run_migrations(pool) -> None:
    """Run database migrations with proper error handling and validation
    
    Raises:
        DatabaseError: If migrations fail
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Get current version
                current_version = await conn.fetchval("""
                    SELECT version FROM db_version 
                    ORDER BY updated_at DESC LIMIT 1
                """)
                
                if current_version is None:
                    current_version = 0
                    
                # Get all migrations after current version
                pending_migrations = {v: sql for v, sql in MIGRATIONS.items() 
                                   if v > current_version}
                
                if not pending_migrations:
                    logger.info(f"Database is up to date at version {current_version}")
                    return
                    
                # Run migrations in version order
                for version in sorted(pending_migrations.keys()):
                    logger.info(f"Running migration to version {version}")
                    
                    # Start migration
                    await conn.execute("""
                        INSERT INTO migration_history (version, started_at)
                        VALUES ($1, CURRENT_TIMESTAMP)
                    """, version)
                    
                    try:
                        # Run migration SQL
                        await conn.execute(pending_migrations[version], version)
                        
                        # Update version
                        await conn.execute("""
                            UPDATE db_version SET version = $1, 
                            updated_at = CURRENT_TIMESTAMP
                        """, version)
                        
                        # Mark migration as completed
                        await conn.execute("""
                            UPDATE migration_history 
                            SET completed_at = CURRENT_TIMESTAMP,
                                success = true
                            WHERE version = $1
                        """, version)
                        
                        logger.info(f"Successfully migrated to version {version}")
                        
                    except Exception as e:
                        # Log migration failure
                        await conn.execute("""
                            UPDATE migration_history 
                            SET completed_at = CURRENT_TIMESTAMP,
                                success = false,
                                error = $2
                            WHERE version = $1
                        """, version, str(e))
                        
                        raise DatabaseError(
                            f"Migration to version {version} failed: {str(e)}"
                        )
                        
    except asyncpg.PostgresError as e:
        raise DatabaseError(f"Database migration failed: {str(e)}")
        
    except Exception as e:
        raise DatabaseError(f"Unexpected error during migration: {str(e)}")

async def init_db():
    """Initialize database and run migrations"""
    try:
        # Initialize database pool
        await get_pool()
        
        # Execute initialization SQL to create tables
        async with _pool.acquire() as conn:
            try:
                await conn.execute(INIT_SQL)
                logger.info("Database tables created successfully")
            except Exception as e:
                logger.error(f"Failed to create database tables: {str(e)}")
                raise
        
        # Run migrations
        try:
            await run_migrations(_pool)
            logger.info("Database migrations completed successfully")
        except Exception as e:
            logger.error(f"Failed to run migrations: {str(e)}")
            raise
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        raise

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
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
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
        user_id: Discord user ID
        
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
        user_id: Discord user ID
        setting: Setting name
        enabled: Whether to enable or disable
        
    Returns:
        bool: Success status
    """
    if setting not in DB_CONFIG['ALLOWED_COLUMNS']['notification_settings']:
        raise ValueError(f"Invalid notification setting: {setting}")
        
    query = f"""
        UPDATE notification_settings 
        SET {setting} = $1 
        WHERE user_id = $2
    """
    
    try:
        await execute_query(query, enabled, user_id)
        return True
    except Exception as e:
        logger.error(f"Failed to update notification setting: {e}")
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
    """Get wallet information with a single optimized query
    
    Args:
        address: Wallet address
        
    Returns:
        Dict containing wallet info, stake address, and delegation status
    """
    query = """
        SELECT w.address, w.stake_address, w.monitoring_since,
               w.last_updated, w.ada_balance,
               d.pool_id as delegation_pool,
               json_build_object(
                   'ada_transactions', ns.ada_transactions,
                   'token_transfers', ns.token_transfers,
                   'nft_updates', ns.nft_updates,
                   'delegation_status', ns.delegation_status,
                   'policy_updates', ns.policy_updates
               ) as notification_settings
        FROM wallets w
        LEFT JOIN delegation_status d ON d.stake_address = w.stake_address
        LEFT JOIN notification_settings ns ON ns.user_id = w.user_id
        WHERE w.address = $1
    """
    try:
        record = await fetch_one(query, address)
        if record:
            return dict(record)
        return {}
    except QueryError as e:
        logger.error(f"Failed to get wallet info: {str(e)}")
        return {}

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
            INSERT INTO delegation_status (stake_address, pool_id)
            SELECT stake_address, $1 FROM wallets WHERE address = $2
            ON CONFLICT (stake_address) DO UPDATE SET pool_id = $1
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

async def get_user_wallets(user_id: str) -> List[Dict[str, Any]]:
    """Get all wallets for a user with optimized query
    
    Args:
        user_id: Discord user ID
        
    Returns:
        List of wallet records with all relevant information
    """
    query = """
        SELECT 
            w.address,
            w.stake_address,
            w.monitoring_since,
            w.ada_balance,
            w.token_balances,
            d.pool_id as delegation_pool,
            (
                SELECT json_agg(json_build_object(
                    'tx_hash', t.tx_hash,
                    'created_at', t.created_at,
                    'metadata', t.metadata
                ))
                FROM (
                    SELECT tx_hash, created_at, metadata
                    FROM transactions
                    WHERE wallet_id = w.id
                    ORDER BY created_at DESC
                    LIMIT 10
                ) t
            ) as recent_transactions
        FROM wallets w
        LEFT JOIN delegation_status d ON d.stake_address = w.stake_address
        WHERE w.user_id = $1
        ORDER BY w.monitoring_since DESC
    """
    try:
        records = await fetch_all(query, user_id)
        return [dict(record) for record in records]
    except QueryError as e:
        logger.error(f"Failed to get user wallets: {str(e)}")
        return []

async def check_ada_balance(address: str) -> tuple[bool, int]:
    """Check if ADA balance is below threshold
    
    Args:
        address: The wallet address
        
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
        address: Wallet address
        balance: Current ADA balance
        
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
        address: Wallet address
        token_balances: Dictionary of token_id -> amount
        
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
        address: Wallet address
        
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
        address: Wallet address
        utxo_state: New UTxO state
        
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
        address: Wallet address
        
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
        address: Wallet address
        stake_address: Stake address
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
        stake_address: Stake address
        epoch: Epoch number
        
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
        stake_address: Stake address
        epoch: Epoch number
        amount: Reward amount in lovelace
        
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
        address: Wallet address
        
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
        address: Wallet address
        
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
        user_id: Discord user ID
        address: Wallet address
        
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
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
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
        wallet_id: Wallet ID
        tx_hash: Transaction hash
        
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
        address: Wallet address
        
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
        address: Wallet address
        
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
        wallet_id: Wallet ID
        
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
        wallet_id: Wallet ID
        
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
        wallet_id: Wallet ID
        
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
        address: Wallet address
        
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
        address: Wallet address
        pool_id: Pool ID
        
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
        policy_id: Policy ID
        
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
        policy_id: Policy ID
        expiry_slot: Slot number when policy expires
        
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
        address: Wallet address
        
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
        address: Wallet address
        tx_hash: Transaction hash
        
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
        address: Wallet address
        
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
        address: Wallet address
        tx_hash: Transaction hash
        
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
        user_id: Discord user ID
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
        stake_address: Stake address
        
    Returns:
        Optional[str]: Discord user ID or None if not found
    """
    query = """
        SELECT DISTINCT user_id 
        FROM wallets 
        WHERE stake_address = $1
    """
    
    try:
        result = await fetch_one(query, stake_address)
        return result['user_id'] if result else None
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
        address: Wallet address
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
        address: Wallet address
        
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
        user_id: Discord user ID
        address: Wallet address to monitor
        stake_address: Stake address
        
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
        user_id: Discord user ID
        address: Wallet address to remove
        
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
        user_id: Discord user ID
        setting: Setting name
        enabled: Whether to enable or disable
        
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
        stake_address: Stake address to look up
        
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
                await asyncio.sleep(retry_delay * (attempt + 1))
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

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
