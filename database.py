import sqlite3
import os
import logging
import asyncio

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_NAME = "wallets.db"  # Default SQLite database name


class DatabaseConnection:
    def __init__(self, db_name=DATABASE_NAME):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    async def __aenter__(self):
        """Async context manager entry"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            return self
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()


async def execute_query(query, params=None, fetch_all=False):
    """Execute a query with proper connection management"""
    async with DatabaseConnection() as db:
        try:
            if params:
                db.cursor.execute(query, params)
            else:
                db.cursor.execute(query)
                
            if fetch_all:
                return db.cursor.fetchall()
            return db.cursor.fetchone()
                
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise


async def init_db():
    """Initialize the SQLite database with proper schema"""
    async with DatabaseConnection() as db:
        try:
            # Create wallets table
            db.cursor.execute("""
                CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    address TEXT NOT NULL,
                    last_tx_hash TEXT,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, address)
                )
            """)
            
            # Create transactions table
            db.cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_id INTEGER NOT NULL,
                    tx_hash TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    block_height INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (wallet_id) REFERENCES wallets(id),
                    UNIQUE(wallet_id, tx_hash)
                )
            """)
            
            logger.info("Database initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            return False

async def add_wallet(address: str, discord_id: str) -> bool:
    """Add a new wallet to monitoring"""
    async with DatabaseConnection() as db:
        try:
            db.cursor.execute(
                """
                INSERT INTO wallets (user_id, address)
                VALUES (?, ?)
                ON CONFLICT (user_id, address) DO UPDATE SET
                    is_active = TRUE,
                    last_checked = CURRENT_TIMESTAMP
                """,
                (discord_id, address)
            )
            logger.info(f"Added/updated wallet {address} for user {discord_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            return False

async def remove_wallet(address: str, discord_id: str) -> bool:
    """Remove a wallet from monitoring"""
    async with DatabaseConnection() as db:
        try:
            db.cursor.execute(
                """
                UPDATE wallets 
                SET is_active = FALSE 
                WHERE address = ? AND user_id = ?
                """,
                (address, discord_id)
            )
            
            affected = db.cursor.rowcount
            if affected > 0:
                logger.info(f"Removed wallet {address} for user {discord_id}")
                return True
            else:
                logger.warning(f"No wallet found to remove: {address} for user {discord_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            return False

async def get_all_wallets():
    """
    Retrieve all wallets from the database.
    :return: List of all wallets.
    """
    try:
        result = await execute_query(
            """
            SELECT * FROM wallets
            """,
            fetch_all=True
        )
        return result
    except Exception as e:
        logger.error(f"Failed to retrieve wallets: {e}")
        return []

async def update_last_checked(wallet_id):
    """
    Update the last checked timestamp for a wallet.
    :param wallet_id: Wallet ID.
    """
    try:
        await execute_query(
            """
            UPDATE wallets
            SET last_checked = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (wallet_id,)
        )
        logger.info(f"Updated last checked for wallet ID: {wallet_id}")
    except Exception as e:
        logger.error(f"Failed to update last checked: {e}")


async def main():
    # Example usage
    try:
        await init_db()
        await add_wallet("addr1...", "1234567890")
        print(await get_all_wallets())
        await remove_wallet("addr1...", "1234567890")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")


if __name__ == "__main__":
    asyncio.run(main())
