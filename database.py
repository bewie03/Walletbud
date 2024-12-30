import os
import logging
import asyncio
import asyncpg

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Pool for database connections
pool = None

async def init_db():
    """Initialize database connection pool and schema"""
    global pool
    try:
        # Create connection pool
        pool = await asyncpg.create_pool(DATABASE_URL)
        
        # Create tables if they don't exist
        async with pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS wallets (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    address TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, address)
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    wallet_id INTEGER REFERENCES wallets(id),
                    tx_hash TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    block_height INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(wallet_id, tx_hash)
                )
            ''')
            
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        return False

async def add_wallet(user_id: str, address: str) -> bool:
    """Add a new wallet"""
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO wallets (user_id, address)
                VALUES ($1, $2)
                ON CONFLICT (user_id, address) 
                DO UPDATE SET is_active = TRUE
            ''', user_id, address)
        return True
        
    except Exception as e:
        logger.error(f"Error adding wallet: {str(e)}")
        return False

async def remove_wallet(user_id: str, address: str) -> bool:
    """Remove a wallet (soft delete)"""
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                UPDATE wallets 
                SET is_active = FALSE 
                WHERE user_id = $1 AND address = $2
            ''', user_id, address)
        return True
        
    except Exception as e:
        logger.error(f"Error removing wallet: {str(e)}")
        return False

async def get_wallet(address: str, user_id: str) -> dict:
    """Get a wallet by address and user ID"""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM wallets 
                WHERE address = $1 AND user_id = $2 AND is_active = TRUE
            ''', address, user_id)
            return dict(row) if row else None
            
    except Exception as e:
        logger.error(f"Error getting wallet: {str(e)}")
        return None

async def get_all_wallets() -> list:
    """Get all active wallets"""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM wallets 
                WHERE is_active = TRUE
                ORDER BY last_checked ASC
            ''')
            return [dict(row) for row in rows]
            
    except Exception as e:
        logger.error(f"Error getting wallets: {str(e)}")
        return []

async def update_last_checked(wallet_id: int) -> bool:
    """Update last checked timestamp"""
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                UPDATE wallets 
                SET last_checked = CURRENT_TIMESTAMP 
                WHERE id = $1
            ''', wallet_id)
        return True
            
    except Exception as e:
        logger.error(f"Error updating last checked: {str(e)}")
        return False

async def add_transaction(wallet_id: int, tx_hash: str, amount: str, block_height: int, timestamp: int) -> bool:
    """Add a new transaction"""
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO transactions (wallet_id, tx_hash, amount, block_height, timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (wallet_id, tx_hash) DO NOTHING
            ''', wallet_id, tx_hash, amount, block_height, timestamp)
        return True
            
    except Exception as e:
        logger.error(f"Error adding transaction: {str(e)}")
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
    asyncio.run(main())
