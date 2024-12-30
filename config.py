import os
from dotenv import load_dotenv

load_dotenv()

def validate_positive_int(value, default, name):
    """Validate and convert environment variable to positive integer"""
    try:
        result = int(value)
        if result <= 0:
            raise ValueError
        return result
    except (ValueError, TypeError):
        print(f"Warning: Invalid {name} value '{value}'. Using default: {default}")
        return default

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN:
    raise ValueError("No Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = '!'

# Blockfrost Configuration
BLOCKFROST_API_KEY = os.getenv('BLOCKFROST_API_KEY')
if not BLOCKFROST_API_KEY:
    raise ValueError("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")

# Use v1 instead of v0
BLOCKFROST_BASE_URL = 'https://cardano-mainnet.blockfrost.io/api/v1'

# Database Configuration
DATABASE_NAME = os.getenv('DATABASE_NAME', 'wallets.db')

# Token Configuration
REQUIRED_BUD_TOKENS = validate_positive_int(
    os.getenv('REQUIRED_BUD_TOKENS', '20000'),
    20000,
    'REQUIRED_BUD_TOKENS'
)

YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID')
if not YUMMI_POLICY_ID:
    raise ValueError("No YUMMI_POLICY_ID found! Make sure YUMMI_POLICY_ID is set in .env")

# Transaction monitoring settings (with validation)
TRANSACTION_CHECK_INTERVAL = validate_positive_int(
    os.getenv('TRANSACTION_CHECK_INTERVAL', '5'),
    5,
    'TRANSACTION_CHECK_INTERVAL'
)

YUMMI_CHECK_INTERVAL = validate_positive_int(
    os.getenv('YUMMI_CHECK_INTERVAL', '6'),
    6,
    'YUMMI_CHECK_INTERVAL'
)

MAX_TX_HISTORY = validate_positive_int(
    os.getenv('MAX_TX_HISTORY', '10'),
    10,
    'MAX_TX_HISTORY'
)

# Rate limiting settings (in seconds)
API_RETRY_DELAY = 2        # Time to wait after rate limit
WALLET_CHECK_DELAY = 60    # Time between wallet check cycles
WALLET_PROCESS_DELAY = 1   # Time between processing each wallet

# Error Messages
ERROR_MESSAGES = {
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Bot API connection is not available. Please try again later.",
    'invalid_address': "Invalid Cardano wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found on the blockchain. Please check the address and try again.",
    'insufficient_tokens': lambda required, current: f"Insufficient YUMMI tokens. Required: {required:,}, Current: {current:,}",
    'wallet_exists': "This wallet is already being monitored!",
    'wallet_not_exists': "This wallet is not being monitored!",
    'monitoring_paused': "Wallet monitoring is currently paused.",
    'db_error': "Database error occurred. Please try again later.",
}
