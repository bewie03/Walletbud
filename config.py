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

# Initialize logger first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('walletbud.config')

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

# Logging configuration
LOG_LEVELS = {
    'bot': logging.INFO,
    'system_commands': logging.INFO,
    'wallet_commands': logging.INFO,
    'webhook': logging.INFO,
    'database': logging.INFO,
    'blockfrost': logging.INFO,
    'shutdown': logging.INFO
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

def validate_webhook_config(config: dict) -> None:
    """Validate webhook configuration values"""
    if not isinstance(config.get('MAX_QUEUE_SIZE'), int) or config['MAX_QUEUE_SIZE'] < 1:
        raise ValueError("MAX_QUEUE_SIZE must be a positive integer")
        
    if not isinstance(config.get('BATCH_SIZE'), int) or config['BATCH_SIZE'] < 1:
        raise ValueError("BATCH_SIZE must be a positive integer")
        
    if not isinstance(config.get('MAX_RETRIES'), int) or config['MAX_RETRIES'] < 0:
        raise ValueError("MAX_RETRIES must be a non-negative integer")
        
    if not isinstance(config.get('MAX_EVENT_AGE'), int) or config['MAX_EVENT_AGE'] < 1:
        raise ValueError("MAX_EVENT_AGE must be a positive integer")
        
    if not isinstance(config.get('CLEANUP_INTERVAL'), int) or config['CLEANUP_INTERVAL'] < 1:
        raise ValueError("CLEANUP_INTERVAL must be a positive integer")
        
    if not isinstance(config.get('MAX_PAYLOAD_SIZE'), int) or config['MAX_PAYLOAD_SIZE'] < 1:
        raise ValueError("MAX_PAYLOAD_SIZE must be a positive integer")
        
    if not isinstance(config.get('MAX_MEMORY_MB'), int) or config['MAX_MEMORY_MB'] < 1:
        raise ValueError("MAX_MEMORY_MB must be a positive integer")
        
    if not isinstance(config.get('RATE_LIMIT_WINDOW'), int) or config['RATE_LIMIT_WINDOW'] < 1:
        raise ValueError("RATE_LIMIT_WINDOW must be a positive integer")
        
    if not isinstance(config.get('RATE_LIMIT_MAX_REQUESTS'), int) or config['RATE_LIMIT_MAX_REQUESTS'] < 1:
        raise ValueError("RATE_LIMIT_MAX_REQUESTS must be a positive integer")
        
    if not isinstance(config.get('RETRY_DELAY'), int) or config['RETRY_DELAY'] < 1:
        raise ValueError("RETRY_DELAY must be a positive integer")

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
    """Validate Blockfrost project ID format"""
    if not value:
        raise ValueError(f"{name}: Project ID cannot be empty")
    
    # Only validate that it's not empty and contains valid characters
    if not re.match('^[a-zA-Z0-9_-]+$', value):
        raise ValueError(f"{name}: Project ID can only contain alphanumeric characters, underscores, and hyphens")
    
    return value

def validate_blockfrost_url(value: str, name: str) -> str:
    """Validate Blockfrost API URL"""
    # Validate against known valid endpoints
    valid_urls = {
        'mainnet': 'https://cardano-mainnet.blockfrost.io/api/v0',
        'preprod': 'https://cardano-preprod.blockfrost.io/api/v0',
        'preview': 'https://cardano-preview.blockfrost.io/api/v0',
        'testnet': 'https://cardano-testnet.blockfrost.io/api/v0'
    }
    
    if value not in valid_urls.values():
        raise ValueError(f"{name}: URL must be one of: {', '.join(valid_urls.values())}")
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
        
    # Validate UUID v4 format
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    if not re.match(uuid_pattern, value.lower()):
        raise ValueError(f"{name} must be a valid UUID v4")
        
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
    """Validate Blockfrost base URL and ensure it matches project ID network"""
    project_id = os.getenv('BLOCKFROST_PROJECT_ID', '')
    if not project_id:
        raise ValueError("BLOCKFROST_PROJECT_ID must be set before validating base URL")
    
    # Extract network from project ID
    network = next((n for n in ['mainnet', 'testnet', 'preview', 'preprod'] if project_id.startswith(n)), None)
    if not network:
        raise ValueError("Invalid BLOCKFROST_PROJECT_ID format")
    
    # Validate URL format
    if not value.startswith('https://'):
        raise ValueError(f"{name}: URL must start with https://")
    
    # Ensure URL matches network
    expected_url = f'https://cardano-{network}.blockfrost.io/api/v0'
    if value != expected_url:
        raise ValueError(f"{name}: URL must be {expected_url} for {network} network")
    
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
        validator=validate_blockfrost_url,
        required=True,
        default="https://cardano-mainnet.blockfrost.io/api/v0"
    ),
    'WEBHOOK_SECRET': EnvVar(
        name='BLOCKFROST_WEBHOOK_SECRET',  # Changed to match Blockfrost's environment variable
        description="Blockfrost webhook verification secret",
        required=True,
        validator=validate_blockfrost_webhook_secret,
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
    'COMMAND_COOLDOWN': EnvVar(
        name='COMMAND_COOLDOWN',
        description="Cooldown between command uses (seconds)",
        default="5",
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
    ),
    'ARCHIVE_AFTER_DAYS': EnvVar(
        name='ARCHIVE_AFTER_DAYS',
        description="Days after which to archive transactions",
        default="30",
        validator=validate_positive_int,
        required=False
    ),
    'DELETE_AFTER_DAYS': EnvVar(
        name='DELETE_AFTER_DAYS',
        description="Days after which to delete archived transactions",
        default="90",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_BATCH_SIZE': EnvVar(
        name='MAINTENANCE_BATCH_SIZE',
        description="Number of records to process in each maintenance batch",
        default="1000",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_MAX_RETRIES': EnvVar(
        name='MAINTENANCE_MAX_RETRIES',
        description="Maximum number of retries for maintenance tasks",
        default="3",
        validator=validate_positive_int,
        required=False
    ),
    'MAINTENANCE_HOUR': EnvVar(
        name='MAINTENANCE_HOUR',
        description="Hour of the day to run maintenance (0-23)",
        default="3",
        validator=validate_hour,
        required=False
    ),
    'MAINTENANCE_MINUTE': EnvVar(
        name='MAINTENANCE_MINUTE',
        description="Minute of the hour to run maintenance (0-59)",
        default="0",
        validator=validate_minute,
        required=False
    ),
    'HEALTH_METRICS_TTL': EnvVar(
        name='HEALTH_METRICS_TTL',
        description="Time-to-live in seconds for health metrics cache",
        default="300",  # 5 minutes default
        validator=validate_positive_int,
    ),
    'MINIMUM_YUMMI': EnvVar(
        name='MINIMUM_YUMMI',
        description="Minimum YUMMI tokens required",
        default="25000",
        validator=validate_positive_int,
        required=False
    ),
    'YUMMI_POLICY_ID': EnvVar(
        name='YUMMI_POLICY_ID',
        description="Policy ID for YUMMI tokens",
        validator=validate_policy_id,
        required=True
    ),
    'YUMMI_TOKEN_NAME': EnvVar(
        name='YUMMI_TOKEN_NAME',
        description="YUMMI Token Name (hex)",
        validator=validate_token_name,
        required=False,
        default=None
    ),
    'COMMAND_COOLDOWN': EnvVar(
        name='COMMAND_COOLDOWN',
        description="Cooldown between command uses (seconds)",
        default="3",
        validator=validate_positive_int,
        required=False
    ),
    'HEALTH_METRICS_TTL': EnvVar(
        name='HEALTH_METRICS_TTL',
        description="Time-to-live in seconds for health metrics cache",
        default="300",  # 5 minutes default
        validator=validate_positive_int,
        required=False
    )
}

# Notification settings
NOTIFICATION_SETTINGS = {
    'balance_alerts': {
        'display_name': 'Balance Alerts',
        'description': 'Get notified when your wallet balance changes'
    },
    'yummi_alerts': {
        'display_name': 'YUMMI Alerts',
        'description': 'Get notified when your YUMMI balance is low'
    },
    'system_alerts': {
        'display_name': 'System Alerts',
        'description': 'Get notified about system status and maintenance'
    }
}

# Export configuration variables
DISCORD_TOKEN = ENV_VARS['DISCORD_TOKEN'].get_value()
APPLICATION_ID = ENV_VARS['APPLICATION_ID'].get_value()
ADMIN_CHANNEL_ID = ENV_VARS['ADMIN_CHANNEL_ID'].get_value()
BLOCKFROST_PROJECT_ID = ENV_VARS['BLOCKFROST_PROJECT_ID'].get_value()
BLOCKFROST_BASE_URL = ENV_VARS['BLOCKFROST_BASE_URL'].get_value()
DATABASE_URL = ENV_VARS['DATABASE_URL'].get_value()
WEBHOOK_SECRET = ENV_VARS['WEBHOOK_SECRET'].get_value()
HEALTH_METRICS_TTL = ENV_VARS['HEALTH_METRICS_TTL'].get_value()

# Rate limiting configuration
MAX_REQUESTS_PER_SECOND = int(os.getenv('MAX_REQUESTS_PER_SECOND', '10'))
BURST_LIMIT = int(os.getenv('BURST_LIMIT', '20'))
RATE_LIMIT_COOLDOWN = int(os.getenv('RATE_LIMIT_COOLDOWN', '60'))

# Health metrics configuration
HEALTH_METRICS_TTL = int(os.getenv('HEALTH_METRICS_TTL', '300'))  # 5 minutes

# Database configuration
MAX_RETRIES = int(os.getenv('DB_MAX_RETRIES', '3'))  # Maximum number of retries
RETRY_DELAY_BASE = int(os.getenv('DB_RETRY_DELAY_BASE', '2'))  # Base for exponential backoff
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Default batch size for database operations

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
}

DATABASE_POOL_MIN_SIZE = 10
DATABASE_POOL_MAX_SIZE = 100
DATABASE_MAX_QUERIES = 50000
DATABASE_CONNECTION_TIMEOUT = 30
DATABASE_COMMAND_TIMEOUT = 60

# Database maintenance configuration
ARCHIVE_AFTER_DAYS = int(os.getenv('ARCHIVE_AFTER_DAYS', '30'))  # Archive transactions after 30 days
DELETE_AFTER_DAYS = int(os.getenv('DELETE_AFTER_DAYS', '90'))    # Delete archived transactions after 90 days
MAINTENANCE_BATCH_SIZE = int(os.getenv('MAINTENANCE_BATCH_SIZE', '1000'))  # Process 1000 records at a time
MAINTENANCE_MAX_RETRIES = int(os.getenv('MAINTENANCE_MAX_RETRIES', '3'))   # Retry failed operations 3 times
MAINTENANCE_HOUR = int(os.getenv('MAINTENANCE_HOUR', '3'))       # Run maintenance at 3 AM
MAINTENANCE_MINUTE = int(os.getenv('MAINTENANCE_MINUTE', '0'))   # Run maintenance at minute 0

# Discord embed limits
EMBED_CHAR_LIMIT = 4096
EMBED_FIELD_LIMIT = 25

# Health check configuration
HEALTH_CHECK_INTERVAL = 60  # seconds
HEALTH_METRICS_TTL = ENV_VARS['HEALTH_METRICS_TTL'].get_value()     # seconds
EMBED_CHAR_LIMIT = 1024     # Discord embed character limit

# Command cooldowns (in seconds)
COMMAND_COOLDOWN = 3

# Balance check configuration
BALANCE_CHECK_INTERVAL = 300  # 5 minutes
YUMMI_WARNING_THRESHOLD = 3   # Number of warnings before reset
MINIMUM_YUMMI = 25000        # Minimum YUMMI tokens required per wallet

# Rate limiting configuration
RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))  # Window in seconds
RATE_LIMIT_MAX_REQUESTS = int(os.getenv('RATE_LIMIT_MAX_REQUESTS', '100'))  # Max requests per window

# Wallet monitoring configuration
WALLET_CHECK_INTERVAL = int(os.getenv('WALLET_CHECK_INTERVAL', '300'))  # Check wallets every 5 minutes
MIN_ADA_BALANCE = float(os.getenv('MIN_ADA_BALANCE', '5.0'))  # Minimum ADA balance to maintain
MAX_TX_PER_HOUR = int(os.getenv('MAX_TX_PER_HOUR', '100'))  # Maximum transactions per hour to process

# YUMMI token configuration
MINIMUM_YUMMI = ENV_VARS['MINIMUM_YUMMI'].get_value()
YUMMI_POLICY_ID = ENV_VARS['YUMMI_POLICY_ID'].get_value()
YUMMI_TOKEN_NAME = ENV_VARS['YUMMI_TOKEN_NAME'].get_value()
ASSET_ID = f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}" if YUMMI_POLICY_ID and YUMMI_TOKEN_NAME else None

# SSL configuration
SSL_CERT_FILE = certifi.where()

# Blockfrost network configuration
BLOCKFROST_NETWORKS = {
    'mainnet': 'https://cardano-mainnet.blockfrost.io/api/v0',
    'testnet': 'https://cardano-testnet.blockfrost.io/api/v0',
    'preview': 'https://cardano-preview.blockfrost.io/api/v0',
    'preprod': 'https://cardano-preprod.blockfrost.io/api/v0'
}

# Blockfrost configuration
BLOCKFROST_PROJECT_ID = os.getenv('BLOCKFROST_PROJECT_ID')
BLOCKFROST_BASE_URL = os.getenv('BLOCKFROST_BASE_URL', 'https://cardano-mainnet.blockfrost.io/api/v0')

# Validate entire configuration
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

# Webhook security configuration
WEBHOOK_SECURITY = {
    'RATE_LIMITS': {
        'global': {
            'requests_per_second': int(os.getenv('WEBHOOK_GLOBAL_RATE_LIMIT', '10')),
            'burst': int(os.getenv('WEBHOOK_GLOBAL_BURST', '20'))
        },
        'per_ip': {
            'requests_per_second': int(os.getenv('WEBHOOK_IP_RATE_LIMIT', '5')),
            'burst': int(os.getenv('WEBHOOK_IP_BURST', '10'))
        }
    },
    'MAX_PAYLOAD_SIZE': int(os.getenv('WEBHOOK_MAX_PAYLOAD_SIZE', '1048576')),  # 1MB
    'ALLOWED_IPS': os.getenv('WEBHOOK_ALLOWED_IPS', '').split(','),
    'BLOCKED_IPS': os.getenv('WEBHOOK_BLOCKED_IPS', '').split(',')
}

# Webhook queue configuration
WEBHOOK_CONFIG = {
    'MAX_QUEUE_SIZE': int(os.getenv('WEBHOOK_MAX_QUEUE_SIZE', '1000')),
    'BATCH_SIZE': int(os.getenv('WEBHOOK_BATCH_SIZE', '10')),
    'MAX_RETRIES': int(os.getenv('WEBHOOK_MAX_RETRIES', '3')),
    'MAX_EVENT_AGE': int(os.getenv('WEBHOOK_MAX_EVENT_AGE', '3600')),  # 1 hour
    'CLEANUP_INTERVAL': int(os.getenv('WEBHOOK_CLEANUP_INTERVAL', '300')),  # 5 minutes
    'MAX_MEMORY_MB': int(os.getenv('WEBHOOK_MAX_MEMORY_MB', '512')),  # 512MB memory limit
    'MAX_PAYLOAD_SIZE': WEBHOOK_SECURITY['MAX_PAYLOAD_SIZE'],  # Use same limit from security config
    'RATE_LIMIT_WINDOW': int(os.getenv('WEBHOOK_RATE_LIMIT_WINDOW', '60')),  # 1 minute window
    'RATE_LIMIT_MAX_REQUESTS': int(os.getenv('WEBHOOK_RATE_LIMIT_MAX_REQUESTS', '60')),  # 60 requests per window
    'RETRY_DELAY': int(os.getenv('WEBHOOK_RETRY_DELAY', '60'))  # 1 minute between retries
}

# Validate webhook configuration
validate_webhook_config(WEBHOOK_CONFIG)
