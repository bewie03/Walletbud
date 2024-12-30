import sqlite3
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_NAME = "wallets.db"  # Default SQLite database name


def init_db():
    """
    Initialize the SQLite database. Creates the necessary tables if they don't exist.
    """
    if not os.access(os.path.dirname(DATABASE_NAME) or '.', os.W_OK):
        raise ValueError(f"Database path '{DATABASE_NAME}' is not writable.")

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Create wallets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                discord_id TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                last_tx_hash TEXT DEFAULT NULL,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(address, discord_id)
            )
        ''')

        # Create transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id INTEGER NOT NULL,
                tx_hash TEXT NOT NULL,
                amount INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                FOREIGN KEY (wallet_id) REFERENCES wallets (id) ON DELETE CASCADE
            )
        ''')

        # Enable foreign key support
        cursor.execute('PRAGMA foreign_keys = ON;')

        conn.commit()
        logger.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def get_db_connection():
    """
    Open a new database connection.
    :return: SQLite connection object.
    """
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries for convenience
        logger.info("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def execute_query(query, params=None):
    """
    Execute a query with optional parameters.
    :param query: SQL query to execute.
    :param params: Parameters for the SQL query.
    :return: Result of the query.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
    except sqlite3.Error as e:
        logger.error(f"Query execution failed: {query}, params: {params}, error: {e}")
        raise


def add_wallet(address, discord_id):
    """
    Add a new wallet to the database.
    :param address: Wallet address.
    :param discord_id: Discord user ID.
    """
    try:
        query = '''
            INSERT INTO wallets (address, discord_id, is_active)
            VALUES (?, ?, TRUE)
        '''
        execute_query(query, (address, discord_id))
        logger.info(f"Wallet added: {address} for Discord ID: {discord_id}")
    except sqlite3.IntegrityError:
        logger.warning(f"Wallet already exists: {address} for Discord ID: {discord_id}")
    except Exception as e:
        logger.error(f"Failed to add wallet: {e}")


def remove_wallet(address, discord_id):
    """
    Remove a wallet from the database.
    :param address: Wallet address.
    :param discord_id: Discord user ID.
    """
    try:
        query = '''
            DELETE FROM wallets
            WHERE address = ? AND discord_id = ?
        '''
        result = execute_query(query, (address, discord_id))
        if result.rowcount == 0:
            logger.warning(f"No wallet found to remove for address: {address} and Discord ID: {discord_id}")
        else:
            logger.info(f"Wallet removed: {address} for Discord ID: {discord_id}")
    except Exception as e:
        logger.error(f"Failed to remove wallet: {e}")


def get_all_wallets():
    """
    Retrieve all wallets from the database.
    :return: List of all wallets.
    """
    try:
        query = '''
            SELECT * FROM wallets
        '''
        cursor = execute_query(query)
        wallets = cursor.fetchall()
        return [dict(wallet) for wallet in wallets]
    except Exception as e:
        logger.error(f"Failed to retrieve wallets: {e}")
        return []


def update_last_checked(wallet_id):
    """
    Update the last checked timestamp for a wallet.
    :param wallet_id: Wallet ID.
    """
    try:
        query = '''
            UPDATE wallets
            SET last_checked = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        '''
        execute_query(query, (wallet_id,))
        logger.info(f"Updated last checked for wallet ID: {wallet_id}")
    except Exception as e:
        logger.error(f"Failed to update last checked: {e}")


if __name__ == "__main__":
    # Example usage
    try:
        init_db()
        add_wallet("addr1...", "1234567890")
        print(get_all_wallets())
        remove_wallet("addr1...", "1234567890")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
