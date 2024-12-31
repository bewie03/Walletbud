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
    return value.lower()  # Always return lowercase

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN or not DISCORD_TOKEN.strip():
    raise ValueError("No valid Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Blockfrost API Configuration
BLOCKFROST_PROJECT_ID = os.getenv('BLOCKFROST_PROJECT_ID')
if not BLOCKFROST_PROJECT_ID or not BLOCKFROST_PROJECT_ID.strip():
    raise ValueError("No valid Blockfrost project ID found! Make sure BLOCKFROST_PROJECT_ID is set in .env")

# YUMMI Token Configuration
try:
    YUMMI_POLICY_ID = validate_hex(
        os.getenv('YUMMI_POLICY_ID', "078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924"),
        56,  # Cardano policy IDs are 56 characters
        "YUMMI_POLICY_ID"
    )
    
    YUMMI_TOKEN_NAME = validate_hex(
        os.getenv('YUMMI_TOKEN_NAME', "59554d4d49"),  # "YUMMI" in hex
        10,  # Token names are typically 10 characters in hex
        "YUMMI_TOKEN_NAME"
    )
    
    YUMMI_TOKEN_ID = f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}"
    
    # Minimum YUMMI token requirement
    MIN_YUMMI_REQUIREMENT = validate_positive_int(
        os.getenv('MIN_YUMMI_REQUIREMENT', 1000),
        1000,  # Default to 1000 YUMMI tokens
        "MIN_YUMMI_REQUIREMENT"
    )
    
    YUMMI_REQUIREMENT = validate_positive_int(
        os.getenv('REQUIRED_YUMMI_TOKENS', '25000'),
        25000,
        "REQUIRED_YUMMI_TOKENS"
    )
except ValueError as e:
    logger.error(f"YUMMI token configuration error: {str(e)}")
    raise

REQUIRED_YUMMI_TOKENS = YUMMI_REQUIREMENT  # Updated threshold

# API Rate Limiting
MAX_REQUESTS_PER_SECOND = validate_positive_int(
    os.getenv('MAX_REQUESTS_PER_SECOND', '10'),
    10,
    "MAX_REQUESTS_PER_SECOND"
)

BURST_LIMIT = validate_positive_int(
    os.getenv('BURST_LIMIT', '500'),
    500,
    "BURST_LIMIT"
)

RATE_LIMIT_COOLDOWN = validate_positive_int(
    os.getenv('RATE_LIMIT_COOLDOWN', '60'),
    60,
    "RATE_LIMIT_COOLDOWN"
)

# Wallet Monitoring
WALLET_CHECK_INTERVAL = validate_positive_int(
    os.getenv('WALLET_CHECK_INTERVAL', '60'),
    60,
    "WALLET_CHECK_INTERVAL"
)

# Wallet Monitoring Configuration
MIN_ADA_BALANCE = 5  # Minimum ADA balance threshold (5 ADA)
MAX_TX_PER_HOUR = 10  # Maximum transactions per hour before alerting
MONITORING_INTERVAL = 60  # Check wallets every 60 seconds

# Logging Settings
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'logs/bot.log'
LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 5

# Load configuration
logger.info(
    "Loaded configuration:\n"
    f"  REQUIRED_YUMMI_TOKENS: {REQUIRED_YUMMI_TOKENS}\n"
    f"  MIN_YUMMI_REQUIREMENT: {MIN_YUMMI_REQUIREMENT}\n"
    f"  MAX_REQUESTS_PER_SECOND: {MAX_REQUESTS_PER_SECOND}\n"
    f"  BURST_LIMIT: {BURST_LIMIT}\n"
    f"  RATE_LIMIT_COOLDOWN: {RATE_LIMIT_COOLDOWN}\n"
    f"  WALLET_CHECK_INTERVAL: {WALLET_CHECK_INTERVAL}\n"
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
