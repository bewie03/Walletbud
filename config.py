import os
import logging
import re
from dotenv import load_dotenv

# Load environment variables
if not load_dotenv():
    print("Warning: .env file not found. Using system environment variables.")

# Set up logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format='%(asctime)s - %(levelname)s - %(message)s')
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

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN or not DISCORD_TOKEN.strip():
    raise ValueError("No valid Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Blockfrost Configuration
BLOCKFROST_API_KEY = os.getenv('BLOCKFROST_API_KEY')
if not BLOCKFROST_API_KEY or not BLOCKFROST_API_KEY.strip():
    raise ValueError("No valid Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")

# Database Configuration
DATABASE_NAME = os.getenv('DATABASE_NAME', 'wallets.db')
if not os.access(os.path.dirname(DATABASE_NAME) or '.', os.W_OK):
    raise ValueError(f"Database path {DATABASE_NAME} is not writable.")

# Token Configuration
REQUIRED_YUMMI_TOKENS = validate_positive_int(
    os.getenv('REQUIRED_YUMMI_TOKENS', '20000'),
    20000,
    'REQUIRED_YUMMI_TOKENS'
)

YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID')
if not YUMMI_POLICY_ID:
    raise ValueError("No YUMMI_POLICY_ID found! Make sure YUMMI_POLICY_ID is set in .env")
if not re.fullmatch(r"(?i)[a-f0-9]{56}", YUMMI_POLICY_ID):
    raise ValueError(f"Invalid YUMMI_POLICY_ID format: {YUMMI_POLICY_ID}. Must be a 56-character hexadecimal string.")

# Monitoring Settings
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
    10,
    'MAX_TX_HISTORY'
)

# Rate Limiting
API_RETRY_DELAY = validate_positive_int(
    os.getenv('API_RETRY_DELAY', '2'),
    2,
    'API_RETRY_DELAY'
)
WALLET_CHECK_DELAY = validate_positive_int(
    os.getenv('WALLET_CHECK_DELAY', '60'),
    60,
    'WALLET_CHECK_DELAY'
)
WALLET_PROCESS_DELAY = validate_positive_int(
    os.getenv('WALLET_PROCESS_DELAY', '1'),
    1,
    'WALLET_PROCESS_DELAY'
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

# Log loaded configuration (excluding sensitive info)
logger.info(f"Loaded configuration:\nDATABASE_NAME: {DATABASE_NAME}\n"
            f"REQUIRED_YUMMI_TOKENS: {REQUIRED_YUMMI_TOKENS}\n"
            f"TRANSACTION_CHECK_INTERVAL: {TRANSACTION_CHECK_INTERVAL}s\n"
            f"WALLET_CHECK_INTERVAL: {WALLET_CHECK_INTERVAL}s\n"
            f"YUMMI_CHECK_INTERVAL: {YUMMI_CHECK_INTERVAL} hours\n")
