import os
import re
import ssl
import logging
import certifi
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Any, Callable, Optional
from ipaddress import ip_network
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# SSL Configuration
SSL_CONFIG = {
    'verify': True,
    'cert_required': True,
    'cert_path': certifi.where(),
    'check_hostname': True
}

# Environment and logging configuration
ENV = os.getenv('ENV', 'development')
LOG_LEVEL = logging.DEBUG if ENV == 'development' else logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Ensure log directory exists
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Logging paths with validation
LOG_PATHS = {
    'bot': os.path.join(LOG_DIR, 'bot.log'),
    'error': os.path.join(LOG_DIR, 'error.log'),
    'webhook': os.path.join(LOG_DIR, 'webhook.log')
}

# Test addresses for different networks
TEST_ADDRESSES = {
    'mainnet': os.getenv('TEST_ADDRESS_MAINNET', 'addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz'),
    'testnet': os.getenv('TEST_ADDRESS_TESTNET', 'addr_test1qp09cl0hpvz5rn9zeac0gkrf5hn25ven9m6u8qh4hi0v8g99vxwvze9nnkth7p3g2m5e7hv4f3p9kjkccy5mtc77q6wsd6qm3e'),
    'preview': os.getenv('TEST_ADDRESS_PREVIEW', 'addr_test1qp09cl0hpvz5rn9zeac0gkrf5hn25ven9m6u8qh4hi0v8g99vxwvze9nnkth7p3g2m5e7hv4f3p9kjkccy5mtc77q6wsd6qm3e')
}

# Rate limits and timeouts with proper validation
RATE_LIMITS = {
    'blockfrost': {
        'calls_per_second': int(os.getenv('BLOCKFROST_RATE_LIMIT', '10')),
        'burst': int(os.getenv('BLOCKFROST_BURST_LIMIT', '50')),
        'timeout': int(os.getenv('BLOCKFROST_TIMEOUT', '30'))
    },
    'discord': {
        'global_rate_limit': int(os.getenv('DISCORD_GLOBAL_RATE_LIMIT', '50')),
        'command_rate_limit': int(os.getenv('DISCORD_COMMAND_RATE_LIMIT', '5')),
        'timeout': int(os.getenv('DISCORD_TIMEOUT', '30'))
    },
    'webhook': {
        'requests_per_minute': int(os.getenv('WEBHOOK_RATE_LIMIT', '60')),
        'burst': int(os.getenv('WEBHOOK_BURST_LIMIT', '10')),
        'timeout': int(os.getenv('WEBHOOK_TIMEOUT', '30'))
    }
}

# Webhook configuration
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')  # Secret for validating webhook signatures
MAX_WEBHOOK_SIZE = int(os.getenv('MAX_WEBHOOK_SIZE', '1048576'))  # 1MB max payload size
MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE', '10000'))  # Maximum number of events in queue
MAX_EVENT_AGE = int(os.getenv('MAX_EVENT_AGE', '86400'))  # Maximum age of events in seconds (24 hours)
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Number of events to process in a batch
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))  # Maximum number of retry attempts
WEBHOOK_RATE_LIMIT = int(os.getenv('WEBHOOK_RATE_LIMIT', '100'))  # Max webhook requests per minute
PROCESS_INTERVAL = int(os.getenv('PROCESS_INTERVAL', '5'))  # Webhook processing interval in seconds
MAX_ERROR_HISTORY = int(os.getenv('MAX_ERROR_HISTORY', '1000'))  # Maximum number of errors to keep in history
WEBHOOK_IDENTIFIER = os.getenv('WEBHOOK_IDENTIFIER', 'WalletBud')  # Identifier for webhook requests

# Rate limiting configuration
RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))  # Window in seconds
RATE_LIMIT_MAX_REQUESTS = int(os.getenv('RATE_LIMIT_MAX_REQUESTS', '100'))  # Max requests per window

# Wallet monitoring configuration
WALLET_CHECK_INTERVAL = int(os.getenv('WALLET_CHECK_INTERVAL', '300'))  # Check wallets every 5 minutes
MIN_ADA_BALANCE = float(os.getenv('MIN_ADA_BALANCE', '5.0'))  # Minimum ADA balance to maintain
MAX_TX_PER_HOUR = int(os.getenv('MAX_TX_PER_HOUR', '100'))  # Maximum transactions per hour to process

# YUMMI token configuration
MINIMUM_YUMMI = int(os.getenv('MINIMUM_YUMMI', '1000000'))  # Minimum YUMMI tokens to hold
YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID')  # YUMMI token policy ID
YUMMI_TOKEN_NAME = os.getenv('YUMMI_TOKEN_NAME')  # YUMMI token name
ASSET_ID = f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}" if YUMMI_POLICY_ID and YUMMI_TOKEN_NAME else None

# Blockfrost network configuration
BLOCKFROST_NETWORKS = {
    'mainnet': 'https://cardano-mainnet.blockfrost.io/api/v0',
    'testnet': 'https://cardano-testnet.blockfrost.io/api/v0',
    'preview': 'https://cardano-preview.blockfrost.io/api/v0',
    'preprod': 'https://cardano-preprod.blockfrost.io/api/v0'
}

def validate_positive_int(value: str, name: str) -> int:
    """Validate and convert to positive integer"""
    try:
        result = int(value)
        if result <= 0:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a positive integer, got: {value}")

def validate_url(value: str, name: str) -> str:
    """Validate URL format"""
    try:
        result = urlparse(value)
        if not all([result.scheme, result.netloc]):
            raise ValueError
        return value
    except ValueError:
        raise ValueError(f"{name} must be a valid URL")

def validate_database_url(value: str, name: str) -> str:
    """Validate PostgreSQL database URL"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
    try:
        result = urlparse(value)
        if not all([result.scheme, result.netloc]):
            raise ValueError
        if result.scheme not in ['postgres', 'postgresql']:
            raise ValueError(f"{name} must use postgres:// or postgresql:// scheme")
        return value
    except ValueError as e:
        raise ValueError(f"{name} must be a valid PostgreSQL URL: {str(e)}")

def validate_discord_token(value: str, name: str) -> str:
    """Validate Discord token format"""
    if not value or len(value) < 10:  # Basic length check
        raise ValueError(f"{name} must be at least 10 characters long")
    return value

def validate_blockfrost_id(value: str, name: str) -> str:
    """Validate Blockfrost project ID format"""
    if not value or len(value) < 32:
        raise ValueError(f"{name} must be at least 32 characters long")
    return value

def validate_blockfrost_project_id(value: str, name: str) -> str:
    """Validate Blockfrost project ID format and network"""
    networks = ['mainnet', 'testnet', 'preview', 'preprod']
    if not any(value.startswith(network) for network in networks):
        raise ValueError(f"Project ID must start with one of: {', '.join(networks)}")
    if not re.match(r'^[a-z]+[A-Za-z0-9]{32}$', value):
        raise ValueError("Invalid project ID format")
    return value

def validate_blockfrost_url(value: str, name: str) -> str:
    """Validate Blockfrost API URL"""
    valid_urls = list(BLOCKFROST_NETWORKS.values())
    if value not in valid_urls:
        raise ValueError(f"Must be one of: {', '.join(valid_urls)}")
    return value

def validate_blockfrost_webhook_id(value: str, name: str) -> str:
    """Validate Blockfrost webhook ID format"""
    if not value or not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', value):
        raise ValueError(f"{name} must be a valid Blockfrost webhook ID, got: {value}")
    return value

def validate_blockfrost_webhook_secret(value: str, name: str) -> str:
    """Validate webhook secret format and strength"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Check minimum length
    if len(value) < 32:
        raise ValueError(f"{name} must be at least 32 characters long")
        
    # Check character set (allow UUID format)
    if not re.match(r'^[0-9a-f-]+$', value):
        raise ValueError(f"{name} must be a valid UUID format")
        
    return value

def validate_ip_ranges(value: str, name: str) -> list:
    """Validate IP ranges in CIDR notation"""
    if not value:
        logger.warning("No IP ranges specified - all IPs will be allowed")
        return []
        
    try:
        ranges = json.loads(value)
        if not isinstance(ranges, list):
            raise ValueError("IP ranges must be a JSON array")
            
        validated_ranges = []
        for ip_range in ranges:
            try:
                # Validate CIDR notation
                network = ip_network(ip_range, strict=False)
                validated_ranges.append(str(network))
            except ValueError as e:
                raise ValueError(f"Invalid IP range '{ip_range}': {str(e)}")
                
        if not validated_ranges:
            logger.warning("Empty IP range list - all IPs will be allowed")
            
        return validated_ranges
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse IP ranges JSON: {str(e)}")

def validate_base_url(value: str, name: str) -> str:
    """Validate Blockfrost base URL"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Known valid endpoints (from official docs)
    valid_endpoints = {
        "https://cardano-mainnet.blockfrost.io/api/v0": "mainnet",
        "https://cardano-testnet.blockfrost.io/api/v0": "testnet", 
        "https://cardano-preview.blockfrost.io/api/v0": "preview",
        "https://cardano-preprod.blockfrost.io/api/v0": "preprod"
    }
    
    # Normalize URL
    value = value.rstrip('/')
    
    if value not in valid_endpoints:
        raise ValueError(f"Must be one of: {', '.join(valid_endpoints)}")
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

def validate_asset_id(value: str, name: str) -> str:
    """Validate Cardano asset ID format"""
    try:
        value = value.strip()
        if not re.match(r'^[0-9a-fA-F]{56,}$', value):
            raise ValueError
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid Cardano asset ID (hex format), got: {value}")

def validate_policy_id(value: str, name: str) -> str:
    """Validate policy ID format"""
    if not value:
        return value
    try:
        value = value.strip()
        if not re.match(r'^[0-9a-fA-F]{56}$', value):
            raise ValueError(f"{name} must be a 56-character hexadecimal string")
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid 56-character hexadecimal string")

def validate_token_name(value: str, name: str) -> str:
    """Validate token name format"""
    if not value:
        return value
    try:
        value = value.strip()
        # Token name should be valid hex and not too long (max 32 bytes = 64 chars)
        if not re.match(r'^[0-9a-fA-F]{1,64}$', value):
            raise ValueError(f"{name} must be a valid hex string (1-64 characters)")
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid hexadecimal string")

def validate_hex(value: str, length: int, name: str) -> str:
    """Validate hexadecimal string of specific length"""
    if not re.match(f'^[0-9a-fA-F]{{{length}}}$', value):
        raise ValueError(f"{name} must be a {length}-character hexadecimal string")
    return value.lower()

@dataclass
class EnvVar:
    """Environment variable configuration with validation"""
    name: str
    description: str = ""
    required: bool = True
    default: Any = None
    validator: Optional[Callable] = None
    sensitive: bool = False

    def get_value(self) -> Any:
        """Get validated environment variable value"""
        value = os.getenv(self.name, self.default)
        
        if self.required and not value:
            raise ValueError(f"Required environment variable {self.name} ({self.description}) is not set")
            
        if value and self.validator:
            try:
                return self.validator(value, self.name)
            except Exception as e:
                raise ValueError(f"Invalid {self.name}: {str(e)}")
                
        return value

# Required environment variables with validation
ENV_VARS = {
    'DISCORD_TOKEN': EnvVar(
        name='DISCORD_TOKEN',
        description="Discord bot token",
        validator=validate_discord_token,
        required=True,
        sensitive=True
    ),
    'DATABASE_URL': EnvVar(
        name='DATABASE_URL',
        description="PostgreSQL connection URL",
        validator=validate_database_url,
        required=True,
        sensitive=True
    ),
    'BLOCKFROST_PROJECT_ID': EnvVar(
        name='BLOCKFROST_PROJECT_ID',
        description="Blockfrost project ID",
        validator=validate_blockfrost_project_id,
        required=True,
        sensitive=True
    ),
    'BLOCKFROST_BASE_URL': EnvVar(
        name='BLOCKFROST_BASE_URL',
        description="Blockfrost API base URL",
        validator=validate_base_url
    ),
    'WEBHOOK_SECRET': EnvVar(
        name='WEBHOOK_SECRET',
        description="Webhook verification secret",
        required=False,
        default="default_webhook_secret",
        sensitive=True
    ),
    'APPLICATION_ID': EnvVar(
        name='APPLICATION_ID',
        description="Discord application ID for the bot",
        required=True,
        validator=lambda x, _: str(int(x)),  # Ensure it's a valid integer
        sensitive=True
    ),
    'ADMIN_CHANNEL_ID': EnvVar(
        name='ADMIN_CHANNEL_ID',
        description="Discord admin channel ID",
        validator=lambda x, _: str(int(x))  # Ensure it's a valid integer
    ),
    'MAX_REQUESTS_PER_SECOND': EnvVar(
        name='MAX_REQUESTS_PER_SECOND',
        description="Maximum API requests per second",
        default="10",
        validator=validate_positive_int,
        required=False
    ),
    'BURST_LIMIT': EnvVar(
        name='BURST_LIMIT',
        description="Maximum burst requests allowed",
        default="20",
        validator=validate_positive_int,
        required=False
    ),
    'RATE_LIMIT_COOLDOWN': EnvVar(
        name='RATE_LIMIT_COOLDOWN',
        description="Rate limit cooldown in seconds",
        default="60",
        validator=validate_positive_int,
        required=False
    ),
    'RATE_LIMIT_WINDOW': EnvVar(
        name='RATE_LIMIT_WINDOW',
        description="Rate limit window in seconds",
        default="60",
        validator=validate_positive_int,
        required=False
    ),
    'RATE_LIMIT_MAX_REQUESTS': EnvVar(
        name='RATE_LIMIT_MAX_REQUESTS',
        description="Maximum requests per rate limit window",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_QUEUE_SIZE': EnvVar(
        name='MAX_QUEUE_SIZE',
        description="Maximum webhook queue size",
        default="1000",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_RETRIES': EnvVar(
        name='MAX_RETRIES',
        description="Maximum number of retries for failed operations",
        default="3",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_EVENT_AGE': EnvVar(
        name='MAX_EVENT_AGE',
        description="Maximum event age in seconds",
        default="3600",
        validator=validate_positive_int,
        required=False
    ),
    'BATCH_SIZE': EnvVar(
        name='BATCH_SIZE',
        description="Batch size for processing operations",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_WEBHOOK_SIZE': EnvVar(
        name='MAX_WEBHOOK_SIZE',
        description="Max webhook size in bytes",
        default="1048576",  # 1MB
        validator=validate_positive_int,
        required=False
    ),
    'WEBHOOK_RATE_LIMIT': EnvVar(
        name='WEBHOOK_RATE_LIMIT',
        description="Maximum webhooks per minute",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'PROCESS_INTERVAL': EnvVar(
        name='PROCESS_INTERVAL',
        description="Queue processing interval in seconds",
        default="5",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_ERROR_HISTORY': EnvVar(
        name='MAX_ERROR_HISTORY',
        description="Maximum errors to keep in history",
        default="1000",
        validator=validate_positive_int,
        required=False
    ),
    'WALLET_CHECK_INTERVAL': EnvVar(
        name='WALLET_CHECK_INTERVAL',
        description="Wallet check interval in seconds",
        default="300",
        validator=validate_positive_int,
        required=False
    ),
    'MIN_ADA_BALANCE': EnvVar(
        name='MIN_ADA_BALANCE',
        description="Minimum ADA balance for alerts",
        default="5.0",
        validator=lambda x, _: float(x)
    ),
    'MAX_TX_PER_HOUR': EnvVar(
        name='MAX_TX_PER_HOUR',
        description="Maximum transactions per hour",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'MINIMUM_YUMMI': EnvVar(
        name='MINIMUM_YUMMI',
        description="Minimum YUMMI tokens required",
        default="1000",
        validator=validate_positive_int,
        required=False
    ),
    'YUMMI_POLICY_ID': EnvVar(
        name='YUMMI_POLICY_ID',
        description="YUMMI Policy ID (56-character hex)",
        validator=validate_policy_id,
        required=False,
        default=None
    ),
    'YUMMI_TOKEN_NAME': EnvVar(
        name='YUMMI_TOKEN_NAME',
        description="YUMMI Token Name (hex)",
        validator=validate_token_name,
        required=False,
        default=None
    ),
    'RATE_LIMIT_WINDOW': EnvVar(
        name='RATE_LIMIT_WINDOW',
        description="Rate limit window in seconds",
        default="60",
        validator=validate_positive_int,
        required=False
    ),
    'RATE_LIMIT_MAX_REQUESTS': EnvVar(
        name='RATE_LIMIT_MAX_REQUESTS',
        description="Maximum requests per rate limit window",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_QUEUE_SIZE': EnvVar(
        name='MAX_QUEUE_SIZE',
        description="Maximum webhook queue size",
        default="1000",
        validator=validate_positive_int,
        required=False
    ),
    'MAX_EVENT_AGE': EnvVar(
        name='MAX_EVENT_AGE',
        description="Maximum event age in seconds",
        default="3600",
        validator=validate_positive_int,
        required=False
    ),
    'COMMAND_COOLDOWN': EnvVar(
        name='COMMAND_COOLDOWN',
        description="Command cooldown in seconds",
        default="60",
        validator=validate_positive_int,
        required=False
    ),
    'ARCHIVE_AFTER_DAYS': EnvVar(
        name='ARCHIVE_AFTER_DAYS',
        description="Days after which to archive events",
        default="30",
        validator=validate_positive_int,
        required=False
    ),
    'DELETE_AFTER_DAYS': EnvVar(
        name='DELETE_AFTER_DAYS',
        description="Days after which to delete events",
        default="90",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_BATCH_SIZE': EnvVar(
        name='MAINTENANCE_BATCH_SIZE',
        description="Batch size for maintenance operations",
        default="100",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_MAX_RETRIES': EnvVar(
        name='MAINTENANCE_MAX_RETRIES',
        description="Maximum retries for maintenance operations",
        default="3",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_HOUR': EnvVar(
        name='MAINTENANCE_HOUR',
        description="Hour of the day to perform maintenance",
        default="2",
        validator=validate_hour,
        required=False
    ),
    'MAINTENANCE_MINUTE': EnvVar(
        name='MAINTENANCE_MINUTE',
        description="Minute of the hour to perform maintenance",
        default="0",
        validator=validate_minute,
        required=False
    ),
    'WEBHOOK_IDENTIFIER': EnvVar(
        name='WEBHOOK_IDENTIFIER',
        description="Identifier for webhook requests",
        default="WalletBud",
        required=False
    ),
    'WEBHOOK_RETRY_ATTEMPTS': EnvVar(
        name='WEBHOOK_RETRY_ATTEMPTS',
        description="Number of retry attempts for webhook requests",
        default="3",
        validator=validate_positive_int,
        required=False
    )
}

# Export configuration variables
DISCORD_TOKEN = ENV_VARS['DISCORD_TOKEN'].get_value()
APPLICATION_ID = ENV_VARS['APPLICATION_ID'].get_value()
ADMIN_CHANNEL_ID = ENV_VARS['ADMIN_CHANNEL_ID'].get_value()
BLOCKFROST_PROJECT_ID = ENV_VARS['BLOCKFROST_PROJECT_ID'].get_value()
BLOCKFROST_BASE_URL = ENV_VARS['BLOCKFROST_BASE_URL'].get_value()
WEBHOOK_SECRET = ENV_VARS['WEBHOOK_SECRET'].get_value()

# Rate limiting configuration
MAX_REQUESTS_PER_SECOND = ENV_VARS['MAX_REQUESTS_PER_SECOND'].get_value()
BURST_LIMIT = ENV_VARS['BURST_LIMIT'].get_value()
RATE_LIMIT_COOLDOWN = ENV_VARS['RATE_LIMIT_COOLDOWN'].get_value()

# Rate limiting and queue configuration
RATE_LIMIT_WINDOW = ENV_VARS['RATE_LIMIT_WINDOW'].get_value()
RATE_LIMIT_MAX_REQUESTS = ENV_VARS['RATE_LIMIT_MAX_REQUESTS'].get_value()
MAX_QUEUE_SIZE = ENV_VARS['MAX_QUEUE_SIZE'].get_value()
MAX_RETRIES = ENV_VARS['MAX_RETRIES'].get_value()
MAX_EVENT_AGE = ENV_VARS['MAX_EVENT_AGE'].get_value()
BATCH_SIZE = ENV_VARS['BATCH_SIZE'].get_value()
MAX_WEBHOOK_SIZE = ENV_VARS['MAX_WEBHOOK_SIZE'].get_value()
WEBHOOK_RATE_LIMIT = ENV_VARS['WEBHOOK_RATE_LIMIT'].get_value()
PROCESS_INTERVAL = ENV_VARS['PROCESS_INTERVAL'].get_value()
MAX_ERROR_HISTORY = ENV_VARS['MAX_ERROR_HISTORY'].get_value()

# Wallet monitoring configuration
WALLET_CHECK_INTERVAL = ENV_VARS['WALLET_CHECK_INTERVAL'].get_value()
MIN_ADA_BALANCE = ENV_VARS['MIN_ADA_BALANCE'].get_value()
MAX_TX_PER_HOUR = ENV_VARS['MAX_TX_PER_HOUR'].get_value()

# YUMMI token configuration
MINIMUM_YUMMI = ENV_VARS['MINIMUM_YUMMI'].get_value()
YUMMI_POLICY_ID = ENV_VARS['YUMMI_POLICY_ID'].get_value()
YUMMI_TOKEN_NAME = ENV_VARS['YUMMI_TOKEN_NAME'].get_value()
ASSET_ID = f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}" if YUMMI_POLICY_ID and YUMMI_TOKEN_NAME else None
WEBHOOK_IDENTIFIER = ENV_VARS['WEBHOOK_IDENTIFIER'].get_value()

# Error configuration
ERROR_MESSAGES = {
    'rate_limit': 'Rate limit exceeded. Please try again later.',
    'invalid_address': 'Invalid Cardano address provided.',
    'network_error': 'Network error occurred. Please try again.',
    'database_error': 'Database error occurred. Please try again.',
    'invalid_token': 'Invalid token provided.',
    'insufficient_balance': 'Insufficient balance for operation.',
    'webhook_error': 'Error processing webhook.',
    'invalid_signature': 'Invalid webhook signature.',
    'maintenance': 'System is currently under maintenance.',
    'timeout': 'Operation timed out.',
    'unknown': 'An unknown error occurred.'
}

# Webhook configuration
WEBHOOK_RETRY_ATTEMPTS = ENV_VARS['WEBHOOK_RETRY_ATTEMPTS'].get_value()

# SSL configuration
SSL_CERT_FILE = certifi.where()

# Initialize configuration
try:
    validate_config()
except Exception as e:
    logging.error(f"Configuration validation failed: {e}")
    raise

# Handle Heroku database URL conversion
database_url = os.getenv('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
    os.environ['DATABASE_URL'] = database_url

# Set up logging
log_level = os.getenv('LOG_LEVEL', 'DEBUG').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.DEBUG),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'MIN_POOL_SIZE': int(os.getenv('DB_MIN_POOL_SIZE', '2')),
    'MAX_POOL_SIZE': int(os.getenv('DB_MAX_POOL_SIZE', '10')),
    'MAX_INACTIVE_CONNECTION_LIFETIME': int(os.getenv('DB_MAX_INACTIVE_CONNECTION_LIFETIME', '300')),  # 5 minutes
    'COMMAND_TIMEOUT': int(os.getenv('DB_COMMAND_TIMEOUT', '60')),  # 1 minute
    'POOL_RECYCLE_INTERVAL': int(os.getenv('DB_POOL_RECYCLE_INTERVAL', '3600')),  # 1 hour
    'MAX_QUERIES_PER_POOL': int(os.getenv('DB_MAX_QUERIES_PER_POOL', '50000')),  # Reset pool after 50k queries
    'MAX_POOL_AGE': int(os.getenv('DB_MAX_POOL_AGE', '86400')),  # 24 hours
    'RETRY_ATTEMPTS': int(os.getenv('DB_RETRY_ATTEMPTS', '3')),
    'RETRY_DELAY': int(os.getenv('DB_RETRY_DELAY', '1')),  # 1 second
    'MAX_RETRIES': int(os.getenv('DB_MAX_RETRIES', '3')),  # Maximum number of retries
    'RETRY_DELAY_BASE': int(os.getenv('DB_RETRY_DELAY_BASE', '2')),  # Base for exponential backoff
}

DATABASE_POOL_MIN_SIZE = 10
DATABASE_POOL_MAX_SIZE = 100
DATABASE_MAX_QUERIES = 50000
DATABASE_CONNECTION_TIMEOUT = 30
DATABASE_COMMAND_TIMEOUT = 60

# Discord embed limits
EMBED_CHAR_LIMIT = 4096
EMBED_FIELD_LIMIT = 25

def validate_config():
    """Validate entire configuration"""
    errors = []
    
    # Validate environment variables
    for var_name, env_var in ENV_VARS.items():
        try:
            env_var.get_value()
        except ValueError as e:
            errors.append(str(e))
    
    # Validate log paths
    for log_type, path in LOG_PATHS.items():
        try:
            with open(path, 'a') as f:
                f.write('')
        except Exception as e:
            errors.append(f"Cannot write to {log_type} log at {path}: {str(e)}")
    
    # Validate SSL configuration
    if SSL_CONFIG['verify']:
        if not os.path.exists(SSL_CONFIG['cert_path']):
            errors.append(f"SSL certificate not found at {SSL_CONFIG['cert_path']}")
    
    if errors:
        raise ValueError("Configuration validation failed:\n" + "\n".join(errors))
