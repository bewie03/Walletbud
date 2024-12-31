import os
import logging
import re
from typing import Optional, Any
from dataclasses import dataclass
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

@dataclass
class EnvVar:
    name: str
    required: bool = True
    default: Any = None
    validator: Optional[callable] = None
    description: str = ""

def validate_positive_int(value: str, name: str) -> int:
    """Validate and convert to positive integer"""
    try:
        result = int(value)
        if result <= 0:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a positive integer, got: {value}")

def validate_hex(value: str, length: int, name: str) -> str:
    """Validate hexadecimal string of specific length"""
    if not value or not re.fullmatch(rf"(?i)[a-f0-9]{{{length}}}", value):
        raise ValueError(f"{name} must be a {length}-character hexadecimal string, got: {value}")
    return value.lower()

def validate_url(value: str, name: str) -> str:
    """Validate URL format"""
    if not value or not re.match(r'^[a-zA-Z]+://[^\s/$.?#].[^\s]*$', value):
        raise ValueError(f"{name} must be a valid URL, got: {value}")
    return value

def validate_discord_token(value: str, name: str) -> str:
    """Validate Discord token format"""
    # Strip quotes and whitespace
    value = value.strip().strip('"\'')
    if not value or not re.match(r'^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$', value):
        raise ValueError(f"{name} must be a valid Discord token")
    return value

def validate_blockfrost_id(value: str, name: str) -> str:
    """Validate Blockfrost project ID format"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
    # Log the project ID format for debugging
    logger.info(f"Validating Blockfrost project ID: {value[:8]}...")
    return value

def validate_blockfrost_webhook_id(value: str, name: str) -> str:
    """Validate Blockfrost webhook ID format"""
    if not value or not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', value):
        raise ValueError(f"{name} must be a valid Blockfrost webhook ID, got: {value}")
    return value

# Define required environment variables
ENV_VARS = {
    # Discord Configuration
    'DISCORD_TOKEN': EnvVar(
        name="Discord Bot Token",
        validator=validate_discord_token,
        description="Discord bot token from Discord Developer Portal"
    ),
    'COMMAND_PREFIX': EnvVar(
        name="Command Prefix",
        required=False,
        default="!",
        description="Prefix for bot commands"
    ),
    
    # Blockfrost Configuration
    'BLOCKFROST_PROJECT_ID': EnvVar(
        name="Blockfrost Project ID",
        validator=validate_blockfrost_id,
        description="Project ID from Blockfrost dashboard"
    ),
    'BLOCKFROST_BASE_URL': EnvVar(
        name="Blockfrost Base URL",
        required=False,
        default="https://cardano-mainnet.blockfrost.io/api/v0",
        validator=validate_url,
        description="Blockfrost API base URL"
    ),
    'BLOCKFROST_TX_WEBHOOK_ID': EnvVar(
        name="Blockfrost Transaction Webhook ID",
        validator=validate_blockfrost_webhook_id,
        description="Transaction webhook ID from Blockfrost dashboard"
    ),
    'BLOCKFROST_DEL_WEBHOOK_ID': EnvVar(
        name="Blockfrost Delegation Webhook ID",
        validator=validate_blockfrost_webhook_id,
        description="Delegation webhook ID from Blockfrost dashboard"
    ),
    'BLOCKFROST_WEBHOOK_SECRET': EnvVar(
        name="Blockfrost Webhook Secret",
        description="Webhook secret from Blockfrost dashboard"
    ),
    
    # YUMMI Token Configuration
    'YUMMI_POLICY_ID': EnvVar(
        name="YUMMI Policy ID",
        validator=lambda x, n: validate_hex(x, 56, n),
        description="YUMMI token policy ID (56-character hexadecimal string)"
    ),
    'YUMMI_TOKEN_NAME': EnvVar(
        name="YUMMI Token Name",
        validator=lambda x, n: validate_hex(x, 9, n),
        description="YUMMI token name (9-character hexadecimal string)"
    ),
    'ASSET_ID': EnvVar(
        name="Asset ID",
        validator=lambda x, n: validate_hex(x, 56+9, n),  # policy_id (56) + token_name (9)
        description="Full asset ID (policy_id + hex-encoded token name)"
    ),
    
    # API Rate Limiting
    'MAX_REQUESTS_PER_SECOND': EnvVar(
        name="Max Requests Per Second",
        required=False,
        default="10",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Maximum API requests per second"
    ),
    'BURST_LIMIT': EnvVar(
        name="Burst Limit",
        required=False,
        default="500",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Maximum burst requests allowed"
    ),
    'RATE_LIMIT_COOLDOWN': EnvVar(
        name="Rate Limit Cooldown",
        required=False,
        default="60",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Rate limit cooldown in seconds"
    ),
    
    # Wallet Monitoring
    'WALLET_CHECK_INTERVAL': EnvVar(
        name="Wallet Check Interval",
        required=False,
        default="60",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Interval to check wallets in seconds"
    ),
    
    # Wallet Monitoring Configuration
    'MIN_ADA_BALANCE': EnvVar(
        name="Minimum ADA Balance",
        required=False,
        default="5",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Minimum ADA balance threshold"
    ),
    'MAX_TX_PER_HOUR': EnvVar(
        name="Maximum Transactions Per Hour",
        required=False,
        default="10",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Maximum transactions per hour before alerting"
    ),
    'MONITORING_INTERVAL': EnvVar(
        name="Monitoring Interval",
        required=False,
        default="60",
        validator=lambda x, n: validate_positive_int(x, n),
        description="Check wallets every X seconds"
    ),
    
    # Webhook Security
    'BLOCKFROST_IP_RANGES': EnvVar(
        name="Blockfrost IP Ranges",
        required=False,
        default="",  # Comma-separated list of allowed IPs/ranges
        description="Allowed IP ranges for Blockfrost webhooks"
    ),
    'WEBHOOK_RATE_LIMIT': EnvVar(
        name="Webhook Rate Limit",
        required=False,
        default="100",  # Max webhooks per minute
        validator=lambda x, n: validate_positive_int(x, n),
        description="Maximum webhooks per minute"
    ),
    'MAX_WEBHOOK_SIZE': EnvVar(
        name="Maximum Webhook Size",
        required=False,
        default="1048576",  # 1MB in bytes
        validator=lambda x, n: validate_positive_int(x, n),
        description="Maximum webhook payload size in bytes"
    ),
}

def validate_env_vars() -> dict:
    """Validate all environment variables and return validated values"""
    validated = {}
    errors = []
    
    for key, env_var in ENV_VARS.items():
        value = os.getenv(key, env_var.default)
        
        # Check if required variable is missing
        if env_var.required and not value:
            errors.append(f"Missing required environment variable: {key} ({env_var.description})")
            continue
            
        # Skip validation for optional empty values
        if not env_var.required and not value:
            validated[key] = None
            continue
            
        # Validate value if validator exists
        try:
            if env_var.validator:
                validated[key] = env_var.validator(value, env_var.name)
            else:
                validated[key] = value
        except ValueError as e:
            errors.append(f"Invalid environment variable {key}: {str(e)}")
            
    # If any errors, raise with all error messages
    if errors:
        raise ValueError("Environment validation failed:\n" + "\n".join(errors))
        
    return validated

# Validate environment variables
try:
    env = validate_env_vars()
    
    # Discord Configuration
    DISCORD_TOKEN = env['DISCORD_TOKEN']
    COMMAND_PREFIX = env['COMMAND_PREFIX']
    
    # Blockfrost Configuration
    BLOCKFROST_PROJECT_ID = env['BLOCKFROST_PROJECT_ID']
    BLOCKFROST_BASE_URL = env['BLOCKFROST_BASE_URL']
    BLOCKFROST_TX_WEBHOOK_ID = env['BLOCKFROST_TX_WEBHOOK_ID']
    BLOCKFROST_DEL_WEBHOOK_ID = env['BLOCKFROST_DEL_WEBHOOK_ID']
    BLOCKFROST_WEBHOOK_SECRET = env['BLOCKFROST_WEBHOOK_SECRET']

    # YUMMI Token Configuration
    YUMMI_POLICY_ID = env['YUMMI_POLICY_ID']
    YUMMI_TOKEN_NAME = env['YUMMI_TOKEN_NAME']
    ASSET_ID = env['ASSET_ID']
    
    # Verify ASSET_ID is correctly formed
    if ASSET_ID != YUMMI_POLICY_ID + YUMMI_TOKEN_NAME:
        raise ValueError(
            f"ASSET_ID must be POLICY_ID + TOKEN_NAME\n"
            f"Expected: {YUMMI_POLICY_ID + YUMMI_TOKEN_NAME}\n"
            f"Got: {ASSET_ID}"
        )
    
    MINIMUM_YUMMI = 25000  # Minimum YUMMI tokens required
    WEBHOOK_IDENTIFIER = "WalletBud"
    WEBHOOK_AUTH_TOKEN = env['BLOCKFROST_WEBHOOK_SECRET']
    WEBHOOK_CONFIRMATIONS = 3

    # Rate Limiting
    MAX_REQUESTS_PER_SECOND = int(env['MAX_REQUESTS_PER_SECOND'])
    BURST_LIMIT = int(env['BURST_LIMIT'])
    RATE_LIMIT_COOLDOWN = int(env['RATE_LIMIT_COOLDOWN'])

    # Wallet Monitoring
    WALLET_CHECK_INTERVAL = int(env['WALLET_CHECK_INTERVAL'])
    MIN_ADA_BALANCE = int(env['MIN_ADA_BALANCE'])
    MAX_TX_PER_HOUR = int(env['MAX_TX_PER_HOUR'])
    MONITORING_INTERVAL = int(env['MONITORING_INTERVAL'])

    # Webhook Security
    BLOCKFROST_IP_RANGES = env['BLOCKFROST_IP_RANGES']
    WEBHOOK_RATE_LIMIT = int(env['WEBHOOK_RATE_LIMIT'])
    MAX_WEBHOOK_SIZE = int(env['MAX_WEBHOOK_SIZE'])

except Exception as e:
    logger.error(f"Environment validation failed:\n{str(e)}")
    raise

# Test address for health checks (a known valid mainnet address)
TEST_ADDRESS = "addr1qxqs59lphg8g6qnplr8q6kw2hyzn8c8e3r5jlnwjqppn8k2vllp6xf5qvjgclau0t2q5jz7c7vyvs3x4u2xqm7gaex0s6dd9ay"

# Webhook Configuration
WEBHOOKS = {
    "transaction": {
        "id": BLOCKFROST_TX_WEBHOOK_ID,
        "auth_token": BLOCKFROST_WEBHOOK_SECRET,
        "confirmations": 2  # Wait for 2 confirmations to avoid rollbacks
    },
    "delegation": {
        "id": BLOCKFROST_DEL_WEBHOOK_ID,
        "auth_token": BLOCKFROST_WEBHOOK_SECRET,
        "confirmations": 1  # Delegation changes need only 1 confirmation
    }
}

# Error Messages
ERROR_MESSAGES = {
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Bot API connection is not available. Please try again later.",
    'invalid_address': "Invalid Cardano wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found on the blockchain. Please check the address and try again.",
    'insufficient_tokens': lambda required, current: (
        f"Insufficient YUMMI tokens. Required: {required:,}, Current: {current:,}"
    ),
    'wallet_exists': "This wallet is already being monitored!",
    'wallet_not_exists': "This wallet is not being monitored!",
    'monitoring_paused': "Wallet monitoring is currently paused.",
    'db_error': "Database error occurred. Please try again later.",
}

# Load configuration
logger.info(
    "Loaded configuration:\n"
    f"  MINIMUM_YUMMI: {MINIMUM_YUMMI}\n"
    f"  MAX_REQUESTS_PER_SECOND: {MAX_REQUESTS_PER_SECOND}\n"
    f"  BURST_LIMIT: {BURST_LIMIT}\n"
    f"  RATE_LIMIT_COOLDOWN: {RATE_LIMIT_COOLDOWN}\n"
    f"  WALLET_CHECK_INTERVAL: {WALLET_CHECK_INTERVAL}\n"
    f"  MIN_ADA_BALANCE: {MIN_ADA_BALANCE}\n"
    f"  MAX_TX_PER_HOUR: {MAX_TX_PER_HOUR}\n"
    f"  MONITORING_INTERVAL: {MONITORING_INTERVAL}s\n"
    f"  BLOCKFROST_IP_RANGES: {BLOCKFROST_IP_RANGES}\n"
    f"  WEBHOOK_RATE_LIMIT: {WEBHOOK_RATE_LIMIT}\n"
    f"  MAX_WEBHOOK_SIZE: {MAX_WEBHOOK_SIZE}\n"
)
