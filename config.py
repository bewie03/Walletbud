import os
import logging
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN or not DISCORD_TOKEN.strip():
    raise ValueError("No valid Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Blockfrost API Configuration
BLOCKFROST_PROJECT_ID = os.getenv('BLOCKFROST_PROJECT_ID')
if not BLOCKFROST_PROJECT_ID or not BLOCKFROST_PROJECT_ID.strip():
    raise ValueError("No valid Blockfrost project ID found! Make sure BLOCKFROST_PROJECT_ID is set in .env")

# Set Blockfrost base URL
BLOCKFROST_BASE_URL = "https://cardano-mainnet.blockfrost.io/api/v0"

# YUMMI Token Configuration
YUMMI_POLICY_ID = "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6"
YUMMI_ASSET_NAME = "4d494d4d49"  # hex for "YUMMI"
YUMMI_TOKEN_ID = f"{YUMMI_POLICY_ID}{YUMMI_ASSET_NAME}"
YUMMI_REQUIREMENT = 25000

REQUIRED_YUMMI_TOKENS = YUMMI_REQUIREMENT  # Updated threshold
MAX_REQUESTS_PER_SECOND = int(os.getenv('MAX_REQUESTS_PER_SECOND', '10'))
BURST_LIMIT = int(os.getenv('BURST_LIMIT', '20'))
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', '0.1'))
WALLET_BATCH_SIZE = int(os.getenv('WALLET_BATCH_SIZE', '10'))
WALLET_CHECK_DELAY = float(os.getenv('WALLET_CHECK_DELAY', '1.0'))
TRANSACTION_CHECK_INTERVAL = int(os.getenv('TRANSACTION_CHECK_INTERVAL', '5'))
WALLET_CHECK_INTERVAL = int(os.getenv('WALLET_CHECK_INTERVAL', '5'))
YUMMI_CHECK_INTERVAL = int(os.getenv('YUMMI_CHECK_INTERVAL', '24'))  # hours
MAX_TX_HISTORY = int(os.getenv('MAX_TX_HISTORY', '10'))
API_RETRY_ATTEMPTS = int(os.getenv('API_RETRY_ATTEMPTS', '3'))
API_RETRY_DELAY = float(os.getenv('API_RETRY_DELAY', '1.0'))
WALLET_PROCESS_DELAY = float(os.getenv('WALLET_PROCESS_DELAY', '0.2'))

# Validate required environment variables
if not all([DISCORD_TOKEN, BLOCKFROST_PROJECT_ID]):
    raise ValueError("Missing required environment variables")

# Wallet Monitoring Configuration
MIN_ADA_BALANCE = 5  # Minimum ADA balance threshold (5 ADA)
MAX_TX_PER_HOUR = 10  # Maximum transactions per hour before alerting
MONITORING_INTERVAL = 60  # Check wallets every 60 seconds

# API Retry Configuration

# Wallet Check Settings

# Logging Settings
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'logs/bot.log'
LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 5

# Load configuration
logger.info(
    "Loaded configuration:\n"
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
    f"  MIN_ADA_BALANCE: {MIN_ADA_BALANCE}\n"
    f"  MAX_TX_PER_HOUR: {MAX_TX_PER_HOUR}\n"
    f"  MONITORING_INTERVAL: {MONITORING_INTERVAL}s\n"
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
