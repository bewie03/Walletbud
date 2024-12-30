import os
import logging
import re
from dotenv import load_dotenv

# Load environment variables
if not load_dotenv():
    print("Warning: .env file not found. Using system environment variables.")

# Set up logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_positive_int(value, default, name):
    """Validate and convert environment variable to positive integer"""
    try:
        result = int(value)
        if result <= 0:
            raise ValueError
        return result
    except (ValueError, TypeError):
        logger.warning(f"Invalid {name} value '{value}'. Using default: {default}")
        return default

def validate_hex(value, length, name):
    """Validate hexadecimal string of a specific length"""
    if not value or not re.fullmatch(rf"(?i)[a-f0-9]{{{length}}}", value):
        raise ValueError(f"Invalid {name} format: {value}. Must be a {length}-character hexadecimal string.")
    return value

def validate_sqlite_db_path(path, default, name):
    """Ensure the SQLite database file path is valid or fallback to a default"""
    dir_path = os.path.dirname(path) or '.'
    if not os.path.exists(dir_path):
        logger.warning(f"{name} directory '{dir_path}' does not exist. Using default: {default}")
        return default
    if not os.access(dir_path, os.W_OK):
        logger.warning(f"{name} path '{dir_path}' is not writable. Using default: {default}")
        return default
    return path

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN or not DISCORD_TOKEN.strip():
    raise ValueError("No valid Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Blockfrost API Configuration
BLOCKFROST_API_KEY = os.getenv('BLOCKFROST_API_KEY')
if not BLOCKFROST_API_KEY or not BLOCKFROST_API_KEY.strip():
    raise ValueError("No valid Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")

# YUMMI Token Configuration
YUMMI_POLICY_ID = validate_hex(
    os.getenv('YUMMI_POLICY_ID'),
    56,
    'YUMMI_POLICY_ID'
)

REQUIRED_YUMMI_TOKENS = validate_positive_int(
    os.getenv('REQUIRED_YUMMI_TOKENS', '20000'),
    1000000,
    'REQUIRED_YUMMI_TOKENS'
)

# Database Configuration
DATABASE_NAME = validate_sqlite_db_path(
    os.getenv('DATABASE_NAME', 'wallets.db'),
    'wallets.db',
    'DATABASE_NAME'
)

# Rate Limiting Configuration
MAX_REQUESTS_PER_SECOND = validate_positive_int(
    os.getenv('MAX_REQUESTS_PER_SECOND', '10'),
    10,
    'MAX_REQUESTS_PER_SECOND'
)

BURST_LIMIT = validate_positive_int(
    os.getenv('BURST_LIMIT', '500'),
    500,
    'BURST_LIMIT'
)

BURST_COOLDOWN = BURST_LIMIT / MAX_REQUESTS_PER_SECOND  # 50 seconds
RATE_LIMIT_DELAY = validate_positive_int(
    os.getenv('RATE_LIMIT_DELAY', '0.1'),
    0.1,
    'RATE_LIMIT_DELAY'
)

# Wallet Monitoring Configuration
TRANSACTION_CHECK_INTERVAL = validate_positive_int(
    os.getenv('TRANSACTION_CHECK_INTERVAL', '5'),
    5,
    'TRANSACTION_CHECK_INTERVAL'
)

WALLET_CHECK_INTERVAL = validate_positive_int(
    os.getenv('WALLET_CHECK_INTERVAL', '5'),
    5,
    'WALLET_CHECK_INTERVAL'
)

YUMMI_CHECK_INTERVAL = validate_positive_int(
    os.getenv('YUMMI_CHECK_INTERVAL', '24'),
    24,
    'YUMMI_CHECK_INTERVAL'
)

MAX_TX_HISTORY = validate_positive_int(
    os.getenv('MAX_TX_HISTORY', '10'),
    50,
    'MAX_TX_HISTORY'
)

# API Retry Configuration
API_RETRY_ATTEMPTS = validate_positive_int(
    os.getenv('API_RETRY_ATTEMPTS', '3'),
    3,
    'API_RETRY_ATTEMPTS'
)

API_RETRY_DELAY = validate_positive_int(
    os.getenv('API_RETRY_DELAY', '1'),
    1,
    'API_RETRY_DELAY'
)

# Wallet Check Settings
WALLET_BATCH_SIZE = 10  # Reduced to avoid hitting rate limits
WALLET_CHECK_DELAY = 1.0  # Increased to respect rate limits
WALLET_PROCESS_DELAY = 0.2  # Increased to respect rate limits

# Logging Settings
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'logs/bot.log'
LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 5

# Load configuration
logger.info(
    "Loaded configuration:\n"
    f"  DATABASE_NAME: {DATABASE_NAME}\n"
    f"  REQUIRED_YUMMI_TOKENS: {REQUIRED_YUMMI_TOKENS}\n"
    f"  MAX_REQUESTS_PER_SECOND: {MAX_REQUESTS_PER_SECOND}\n"
    f"  RATE_LIMIT_DELAY: {RATE_LIMIT_DELAY}s\n"
    f"  WALLET_BATCH_SIZE: {WALLET_BATCH_SIZE}\n"
    f"  WALLET_CHECK_DELAY: {WALLET_CHECK_DELAY}s\n"
    f"  TRANSACTION_CHECK_INTERVAL: {TRANSACTION_CHECK_INTERVAL}s\n"
    f"  WALLET_CHECK_INTERVAL: {WALLET_CHECK_INTERVAL}s\n"
    f"  YUMMI_CHECK_INTERVAL: {YUMMI_CHECK_INTERVAL} hours\n"
    f"  MAX_TX_HISTORY: {MAX_TX_HISTORY}\n"
    f"  API_RETRY_DELAY: {API_RETRY_DELAY}s\n"
    f"  WALLET_PROCESS_DELAY: {WALLET_PROCESS_DELAY}s\n"
)

# Error Messages
def get_insufficient_tokens_message(required, current):
    return f"Insufficient YUMMI tokens. Required: {required:,}, Current: {current:,}"

ERROR_MESSAGES = {
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Bot API connection is not available. Please try again later.",
    'invalid_address': "Invalid Cardano wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found on the blockchain. Please check the address and try again.",
    'insufficient_tokens': get_insufficient_tokens_message,
    'wallet_exists': "This wallet is already being monitored!",
    'wallet_not_exists': "This wallet is not being monitored!",
    'monitoring_paused': "Wallet monitoring is currently paused.",
    'db_error': "Database error occurred. Please try again later.",
}
