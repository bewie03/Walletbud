import os
import logging
import re
from typing import Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Handle Heroku database URL conversion
database_url = os.getenv('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
    os.environ['DATABASE_URL'] = database_url

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

def validate_minute(value: str, name: str) -> int:
    """Validate and convert to minute value (0-59)"""
    try:
        result = int(value)
        if result < 0 or result > 59:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be an integer between 0 and 59, got: {value}")

def validate_hour(value: str, name: str) -> int:
    """Validate and convert to hour value (0-23)"""
    try:
        result = int(value)
        if result < 0 or result > 23:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be an integer between 0 and 23, got: {value}")

def validate_ip_ranges(value: str, name: str) -> list:
    """Validate IP ranges in CIDR notation"""
    try:
        ranges = json.loads(value) if isinstance(value, str) else value
        if not isinstance(ranges, list):
            raise ValueError
        
        from ipaddress import ip_network
        validated = []
        for ip_range in ranges:
            # Will raise ValueError if invalid
            network = ip_network(ip_range.strip())
            validated.append(str(network))
        return validated
    except Exception as e:
        raise ValueError(f"{name} must be a list of valid IP ranges in CIDR notation, got: {value}")

def validate_webhook_id(value: str, name: str) -> str:
    """Validate Blockfrost webhook ID format (UUID)"""
    try:
        # Strip any whitespace
        value = value.strip()
        # Verify UUID format
        if not value or not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', value.lower()):
            raise ValueError
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid UUID format, got: {value}")

def validate_webhook_secret(value: str, name: str) -> str:
    """Validate webhook secret format"""
    if not value or len(value) < 32:  # Require at least 32 characters for security
        raise ValueError(f"{name} must be at least 32 characters long")
    return value

def validate_asset_id(value: str, name: str) -> str:
    """Validate Cardano asset ID format"""
    try:
        value = value.strip()
        if not re.match(r'^[0-9a-fA-F]{56,}$', value):
            raise ValueError
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid Cardano asset ID (hex format), got: {value}")

def validate_token_name(value: str, name: str) -> str:
    """Validate token name format"""
    try:
        value = value.strip()
        # Check if it's a valid hex string and not too long
        if not re.match(r'^[0-9a-fA-F]{1,64}$', value):
            raise ValueError
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid hex-encoded token name, got: {value}")

# Define required environment variables
ENV_VARS = {
    # Discord Configuration
    'DISCORD_TOKEN': EnvVar(
        name="Discord Bot Token",
        validator=validate_discord_token,
        description="Discord bot token"
    ),
    'ADMIN_CHANNEL_ID': EnvVar(
        name='ADMIN_CHANNEL_ID',
        required=False,
        description="Discord channel ID for admin notifications"
    ),

    # Blockfrost Configuration
    'BLOCKFROST_PROJECT_ID': EnvVar(
        name='BLOCKFROST_PROJECT_ID',
        validator=validate_blockfrost_id,
        description="Blockfrost API project ID"
    ),
    'BLOCKFROST_BASE_URL': EnvVar(
        name='BLOCKFROST_BASE_URL',
        default="https://cardano-mainnet.blockfrost.io/api/v0",
        validator=validate_url,
        description="Blockfrost API base URL"
    ),
    'BLOCKFROST_IP_RANGES': EnvVar(
        name='BLOCKFROST_IP_RANGES',
        default='["0.0.0.0/0"]',  # Default to allow all IPs
        validator=validate_ip_ranges,
        description="List of allowed Blockfrost webhook IP ranges in CIDR notation"
    ),
    'BLOCKFROST_TX_WEBHOOK_ID': EnvVar(
        name='BLOCKFROST_TX_WEBHOOK_ID',
        validator=validate_webhook_id,
        description="Blockfrost transaction webhook ID"
    ),
    'BLOCKFROST_DEL_WEBHOOK_ID': EnvVar(
        name='BLOCKFROST_DEL_WEBHOOK_ID',
        validator=validate_webhook_id,
        description="Blockfrost delegation webhook ID"
    ),
    'BLOCKFROST_WEBHOOK_SECRET': EnvVar(
        name='BLOCKFROST_WEBHOOK_SECRET',
        validator=validate_webhook_secret,
        description="Blockfrost webhook secret for verification"
    ),

    # Database Configuration
    'DATABASE_URL': EnvVar(
        name='DATABASE_URL',
        description="PostgreSQL connection string"
    ),

    # Rate Limiting Configuration
    'MAX_REQUESTS_PER_SECOND': EnvVar(
        name='MAX_REQUESTS_PER_SECOND',
        default="10",
        validator=validate_positive_int,
        description="Maximum API requests per second"
    ),
    'BURST_LIMIT': EnvVar(
        name='BURST_LIMIT',
        default="500",
        validator=validate_positive_int,
        description="Maximum burst requests allowed"
    ),
    'RATE_LIMIT_COOLDOWN': EnvVar(
        name='RATE_LIMIT_COOLDOWN',
        default="60",
        validator=validate_positive_int,
        description="Rate limit cooldown in seconds"
    ),
    'RATE_LIMIT_WINDOW': EnvVar(
        name='RATE_LIMIT_WINDOW',
        default="60",
        validator=validate_positive_int,
        description="Rate limit window in seconds"
    ),
    'RATE_LIMIT_MAX_REQUESTS': EnvVar(
        name='RATE_LIMIT_MAX_REQUESTS',
        default="100",
        validator=validate_positive_int,
        description="Maximum requests per window"
    ),

    # Queue Configuration
    'MAX_QUEUE_SIZE': EnvVar(
        name='MAX_QUEUE_SIZE',
        default="10000",
        validator=validate_positive_int,
        description="Maximum events in queue"
    ),
    'MAX_RETRIES': EnvVar(
        name='MAX_RETRIES',
        default="5",
        validator=validate_positive_int,
        description="Maximum retry attempts per event"
    ),
    'MAX_EVENT_AGE': EnvVar(
        name='MAX_EVENT_AGE',
        default="3600",
        validator=validate_positive_int,
        description="Maximum age of event in seconds"
    ),
    'BATCH_SIZE': EnvVar(
        name='BATCH_SIZE',
        default="10",
        validator=validate_positive_int,
        description="Process this many events at once"
    ),
    'MAX_WEBHOOK_SIZE': EnvVar(
        name='MAX_WEBHOOK_SIZE',
        default="1048576",  # 1MB
        validator=validate_positive_int,
        description="Maximum webhook payload size in bytes"
    ),
    'WEBHOOK_RATE_LIMIT': EnvVar(
        name='WEBHOOK_RATE_LIMIT',
        default="100",
        validator=validate_positive_int,
        description="Maximum webhooks per minute"
    ),
    'PROCESS_INTERVAL': EnvVar(
        name='PROCESS_INTERVAL',
        default="1",
        validator=validate_positive_int,
        description="Process queue every N seconds"
    ),
    'MAX_ERROR_HISTORY': EnvVar(
        name='MAX_ERROR_HISTORY',
        default="1000",
        validator=validate_positive_int,
        description="Maximum number of errors to keep in history"
    ),

    # Wallet Monitoring Configuration
    'WALLET_CHECK_INTERVAL': EnvVar(
        name='WALLET_CHECK_INTERVAL',
        default="300",
        validator=validate_positive_int,
        description="Interval to check wallets in seconds"
    ),
    'MIN_ADA_BALANCE': EnvVar(
        name='MIN_ADA_BALANCE',
        default="5",
        validator=validate_positive_int,
        description="Minimum ADA balance for alerts"
    ),
    'MAX_TX_PER_HOUR': EnvVar(
        name='MAX_TX_PER_HOUR',
        default="10",
        validator=validate_positive_int,
        description="Maximum transactions per hour"
    ),
    'MINIMUM_YUMMI': EnvVar(
        name="Minimum YUMMI Balance",
        default="1000",
        validator=validate_positive_int,
        required=False,
        description="Minimum YUMMI token balance required"
    ),

    # Maintenance Configuration
    'MAINTENANCE_HOUR': EnvVar(
        name='MAINTENANCE_HOUR',
        default="2",
        validator=validate_hour,
        description="Hour to run maintenance (24-hour format)"
    ),
    'MAINTENANCE_MINUTE': EnvVar(
        name='MAINTENANCE_MINUTE',
        default="0",
        validator=validate_minute,
        description="Minute to run maintenance"
    ),

    # Asset Configuration
    'ASSET_ID': EnvVar(
        name='ASSET_ID',
        validator=validate_asset_id,
        description="Cardano asset ID to monitor"
    ),
    'YUMMI_POLICY_ID': EnvVar(
        name='YUMMI_POLICY_ID',
        validator=validate_asset_id,
        description="YUMMI token policy ID"
    ),
    'YUMMI_TOKEN_NAME': EnvVar(
        name='YUMMI_TOKEN_NAME',
        validator=validate_token_name,
        description="YUMMI token name (hex encoded)"
    ),
    'WEBHOOK_IDENTIFIER': EnvVar(
        name="Webhook Identifier",
        default="walletbud_webhook",
        required=False,
        description="Identifier for the webhook"
    ),
    'WEBHOOK_AUTH_TOKEN': EnvVar(
        name="Webhook Authentication Token",
        default="your_webhook_auth_token",
        required=False,
        description="Authentication token for the webhook"
    ),
    'WEBHOOK_CONFIRMATIONS': EnvVar(
        name="Webhook Confirmations",
        default="3",
        validator=validate_positive_int,
        required=False,
        description="Number of confirmations required for webhook events"
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
    ADMIN_CHANNEL_ID = env['ADMIN_CHANNEL_ID']
    
    # Blockfrost Configuration
    BLOCKFROST_PROJECT_ID = env['BLOCKFROST_PROJECT_ID']
    BLOCKFROST_BASE_URL = env['BLOCKFROST_BASE_URL']
    BLOCKFROST_IP_RANGES = env['BLOCKFROST_IP_RANGES']
    BLOCKFROST_TX_WEBHOOK_ID = env['BLOCKFROST_TX_WEBHOOK_ID']
    BLOCKFROST_DEL_WEBHOOK_ID = env['BLOCKFROST_DEL_WEBHOOK_ID']
    BLOCKFROST_WEBHOOK_SECRET = env['BLOCKFROST_WEBHOOK_SECRET']

    # Database Configuration
    DATABASE_URL = env['DATABASE_URL']

    # Rate Limiting Configuration
    MAX_REQUESTS_PER_SECOND = int(env['MAX_REQUESTS_PER_SECOND'])
    BURST_LIMIT = int(env['BURST_LIMIT'])
    RATE_LIMIT_COOLDOWN = int(env['RATE_LIMIT_COOLDOWN'])
    RATE_LIMIT_WINDOW = int(env['RATE_LIMIT_WINDOW'])
    RATE_LIMIT_MAX_REQUESTS = int(env['RATE_LIMIT_MAX_REQUESTS'])

    # Queue Configuration
    MAX_QUEUE_SIZE = int(env['MAX_QUEUE_SIZE'])
    MAX_RETRIES = int(env['MAX_RETRIES'])
    MAX_EVENT_AGE = int(env['MAX_EVENT_AGE'])
    BATCH_SIZE = int(env['BATCH_SIZE'])
    MAX_WEBHOOK_SIZE = int(env['MAX_WEBHOOK_SIZE'])
    WEBHOOK_RATE_LIMIT = int(env['WEBHOOK_RATE_LIMIT'])
    PROCESS_INTERVAL = int(env['PROCESS_INTERVAL'])
    MAX_ERROR_HISTORY = int(env['MAX_ERROR_HISTORY'])

    # Wallet Monitoring Configuration
    WALLET_CHECK_INTERVAL = int(env['WALLET_CHECK_INTERVAL'])
    MIN_ADA_BALANCE = int(env['MIN_ADA_BALANCE'])
    MAX_TX_PER_HOUR = int(env['MAX_TX_PER_HOUR'])
    MINIMUM_YUMMI = int(env['MINIMUM_YUMMI'])

    # Maintenance Configuration
    MAINTENANCE_HOUR = int(env['MAINTENANCE_HOUR'])
    MAINTENANCE_MINUTE = int(env['MAINTENANCE_MINUTE'])

    # Asset Configuration
    ASSET_ID = env['ASSET_ID']
    YUMMI_POLICY_ID = env['YUMMI_POLICY_ID']
    YUMMI_TOKEN_NAME = env['YUMMI_TOKEN_NAME']
    WEBHOOK_IDENTIFIER = env['WEBHOOK_IDENTIFIER']
    WEBHOOK_AUTH_TOKEN = env['WEBHOOK_AUTH_TOKEN']
    WEBHOOK_CONFIRMATIONS = int(env['WEBHOOK_CONFIRMATIONS'])

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
    f"  MAX_REQUESTS_PER_SECOND: {MAX_REQUESTS_PER_SECOND}\n"
    f"  BURST_LIMIT: {BURST_LIMIT}\n"
    f"  RATE_LIMIT_COOLDOWN: {RATE_LIMIT_COOLDOWN}\n"
    f"  RATE_LIMIT_WINDOW: {RATE_LIMIT_WINDOW}\n"
    f"  RATE_LIMIT_MAX_REQUESTS: {RATE_LIMIT_MAX_REQUESTS}\n"
    f"  MAX_QUEUE_SIZE: {MAX_QUEUE_SIZE}\n"
    f"  MAX_RETRIES: {MAX_RETRIES}\n"
    f"  MAX_EVENT_AGE: {MAX_EVENT_AGE}\n"
    f"  BATCH_SIZE: {BATCH_SIZE}\n"
    f"  MAX_WEBHOOK_SIZE: {MAX_WEBHOOK_SIZE}\n"
    f"  WEBHOOK_RATE_LIMIT: {WEBHOOK_RATE_LIMIT}\n"
    f"  PROCESS_INTERVAL: {PROCESS_INTERVAL}\n"
    f"  MAX_ERROR_HISTORY: {MAX_ERROR_HISTORY}\n"
    f"  WALLET_CHECK_INTERVAL: {WALLET_CHECK_INTERVAL}\n"
    f"  MIN_ADA_BALANCE: {MIN_ADA_BALANCE}\n"
    f"  MAX_TX_PER_HOUR: {MAX_TX_PER_HOUR}\n"
    f"  MINIMUM_YUMMI: {MINIMUM_YUMMI}\n"
    f"  MAINTENANCE_HOUR: {MAINTENANCE_HOUR}\n"
    f"  MAINTENANCE_MINUTE: {MAINTENANCE_MINUTE}\n"
    f"  BLOCKFROST_IP_RANGES: {BLOCKFROST_IP_RANGES}\n"
    f"  ASSET_ID: {ASSET_ID}\n"
    f"  YUMMI_POLICY_ID: {YUMMI_POLICY_ID}\n"
    f"  YUMMI_TOKEN_NAME: {YUMMI_TOKEN_NAME}\n"
    f"  WEBHOOK_IDENTIFIER: {WEBHOOK_IDENTIFIER}\n"
    f"  WEBHOOK_AUTH_TOKEN: {'*' * 8}\n"
    f"  WEBHOOK_CONFIRMATIONS: {WEBHOOK_CONFIRMATIONS}\n"
)
