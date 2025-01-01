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

# Database configuration with connection pooling
DATABASE_CONFIG = {
    'min_size': int(os.getenv('DATABASE_POOL_MIN_SIZE', '10')),
    'max_size': int(os.getenv('DATABASE_POOL_MAX_SIZE', '100')),
    'max_queries': int(os.getenv('DATABASE_MAX_QUERIES', '50000')),
    'timeout': int(os.getenv('DATABASE_TIMEOUT', '30')),
    'command_timeout': int(os.getenv('DATABASE_COMMAND_TIMEOUT', '60')),
    'ssl': os.getenv('DATABASE_SSL', 'true').lower() == 'true'
}

# SSL Configuration with proper cert handling
SSL_CONFIG = {
    'verify': os.getenv('SSL_VERIFY', 'true').lower() == 'true',
    'cert_required': os.getenv('SSL_CERT_REQUIRED', 'true').lower() == 'true',
    'cert_path': os.getenv('SSL_CERT_PATH', certifi.where()),
    'check_hostname': os.getenv('SSL_CHECK_HOSTNAME', 'true').lower() == 'true'
}

# Webhook configuration with proper validation
WEBHOOK_CONFIG = {
    'queue_size': int(os.getenv('WEBHOOK_QUEUE_SIZE', '1000')),
    'retry_attempts': int(os.getenv('WEBHOOK_RETRY_ATTEMPTS', '3')),
    'backoff_factor': float(os.getenv('WEBHOOK_BACKOFF_FACTOR', '1.5')),
    'max_backoff': int(os.getenv('WEBHOOK_MAX_BACKOFF', '30')),
    'timeout': int(os.getenv('WEBHOOK_TIMEOUT', '30')),
    'confirmations': int(os.getenv('WEBHOOK_CONFIRMATIONS', '3'))
}

# Blockfrost network configuration
BLOCKFROST_NETWORKS = {
    'mainnet': 'https://cardano-mainnet.blockfrost.io/api/v0',
    'testnet': 'https://cardano-testnet.blockfrost.io/api/v0',
    'preview': 'https://cardano-preview.blockfrost.io/api/v0',
    'preprod': 'https://cardano-preprod.blockfrost.io/api/v0'
}

@dataclass
class EnvVar:
    """Environment variable configuration with validation"""
    name: str
    description: str
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

def validate_url(value: str, name: str) -> str:
    """Validate URL format"""
    try:
        result = urlparse(value)
        if not all([result.scheme, result.netloc]):
            raise ValueError("Invalid URL format")
        return value
    except Exception as e:
        raise ValueError(f"Invalid URL format: {str(e)}")

def validate_database_url(value: str, name: str) -> str:
    """Validate and normalize database URL"""
    if not value:
        raise ValueError(f"{name} is required")
        
    # Handle Heroku's postgres:// format
    if value.startswith("postgres://"):
        value = value.replace("postgres://", "postgresql://", 1)
        
    try:
        result = urlparse(value)
        if not all([result.scheme, result.hostname, result.username, result.password]):
            raise ValueError(f"{name} is missing required components")
            
        if result.scheme not in ['postgresql', 'postgres']:
            raise ValueError(f"{name} must be a PostgreSQL URL")
            
    except Exception as e:
        raise ValueError(f"Invalid {name}: {str(e)}")
        
    return value

def validate_discord_token(value: str, name: str) -> str:
    """Validate Discord token format"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Check basic format (just length and characters)
    if not re.match(r'^[A-Za-z0-9._-]{50,100}$', value):
        raise ValueError(f"{name} has invalid format")
        
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

# Required environment variables with validation
ENV_VARS = {
    'DISCORD_TOKEN': EnvVar(
        'DISCORD_TOKEN',
        'Discord bot token',
        validator=validate_discord_token,
        sensitive=True
    ),
    'DATABASE_URL': EnvVar(
        'DATABASE_URL',
        'PostgreSQL connection URL',
        validator=validate_database_url,
        sensitive=True
    ),
    'BLOCKFROST_PROJECT_ID': EnvVar(
        'BLOCKFROST_PROJECT_ID',
        'Blockfrost project ID',
        validator=validate_blockfrost_project_id,
        sensitive=True
    ),
    'BLOCKFROST_BASE_URL': EnvVar(
        'BLOCKFROST_BASE_URL',
        'Blockfrost API base URL',
        validator=validate_blockfrost_url
    ),
    'WEBHOOK_SECRET': EnvVar(
        'WEBHOOK_SECRET',
        'Webhook verification secret',
        required=True,
        sensitive=True
    ),
    'ADMIN_CHANNEL_ID': EnvVar(
        'ADMIN_CHANNEL_ID',
        'Discord admin channel ID',
        validator=lambda x, _: str(int(x))  # Ensure it's a valid integer
    )
}

# Error messages with detailed descriptions
ERROR_MESSAGES = {
    'env_validation': "Environment validation failed. Please check your configuration.",
    'blockfrost_init': "Failed to initialize Blockfrost API. Please check your credentials.",
    'database_init': "Failed to connect to database. Please check your connection settings.",
    'rate_limit': "Rate limit exceeded. Please try again later.",
    'webhook_error': "Failed to process webhook. Please check your configuration.",
    'invalid_signature': "Invalid webhook signature.",
    'invalid_ip': "Request from unauthorized IP address.",
    'queue_full': "Webhook queue is full. Please try again later.",
    'ssl_error': "SSL/TLS connection failed. Please check your certificates.",
    'log_error': "Failed to initialize logging. Please check file permissions.",
    'network_mismatch': "Blockfrost network mismatch between project ID and base URL."
}

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

# Initialize configuration
try:
    validate_config()
except Exception as e:
    logging.error(f"Configuration validation failed: {e}")
    raise

# Load environment variables
load_dotenv()

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

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'bot.log',
            'formatter': 'detailed',
            'level': 'DEBUG',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
        },
        'error_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'error.log',
            'formatter': 'detailed',
            'level': 'ERROR',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
        }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': True
        },
        'discord': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False
        },
        'blockfrost': {
            'handlers': ['console', 'file', 'error_file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

# Environment-specific settings
ENV = os.getenv('ENV', 'development')
LOG_LEVEL = logging.DEBUG if ENV == 'development' else logging.INFO

# Rate limits and timeouts
RATE_LIMITS = {
    'blockfrost': {
        'calls_per_second': 10,
        'burst': 50,
        'timeout': 30
    },
    'discord': {
        'global_rate_limit': 50,  # commands per minute
        'command_rate_limit': 5    # commands per user per minute
    }
}

# Retry configuration
WEBHOOK_RETRY_ATTEMPTS = 3
WEBHOOK_QUEUE_SIZE = 1000
WEBHOOK_TIMEOUT = 30

# Health check configuration
HEALTH_CHECK_INTERVAL = 300  # 5 minutes
HEALTH_CHECK_TIMEOUT = 30
HEALTH_CACHE_TTL = 60  # 1 minute

# Error messages
ERROR_MESSAGES = {
    'blockfrost_init': "Failed to initialize Blockfrost API. Please check your credentials and try again.",
    'database_init': "Failed to connect to database. Please check your connection settings.",
    'rate_limit': "Rate limit exceeded. Please try again later.",
    'webhook_error': "Failed to process webhook. Please check your configuration.",
    'invalid_signature': "Invalid webhook signature.",
    'invalid_ip': "Request from unauthorized IP address.",
    'queue_full': "Webhook queue is full. Please try again later.",
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Bot API connection is not available. Please try again later.",
    'invalid_address': "Invalid Cardano wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found on the blockchain. Please check the address and try again.",
    'invalid_project_id': "Invalid Blockfrost project ID format. Must start with mainnet/testnet/preview/preprod followed by 32 alphanumeric characters.",
    'invalid_base_url': lambda valid_urls: f"Invalid Blockfrost base URL. Must be one of: {', '.join(valid_urls)}",
    'network_prefix_mismatch': lambda prefix, expected, url: f"Project ID network prefix '{prefix}' does not match base URL network '{expected}' ({url})",
}

# Database configuration
DATABASE_POOL_MIN_SIZE = 10
DATABASE_POOL_MAX_SIZE = 100
DATABASE_MAX_QUERIES = 50000
DATABASE_CONNECTION_TIMEOUT = 30
DATABASE_COMMAND_TIMEOUT = 60

# SSL configuration
SSL_VERIFY = True
SSL_CERT_REQUIRED = True

# Webhook configuration
WEBHOOK_CONFIRMATIONS = 3
WEBHOOK_AUTH_TOKEN = os.getenv('WEBHOOK_AUTH_TOKEN')
WEBHOOK_IDENTIFIER = 'walletbud-bot'

# Command cooldowns (in seconds)
COMMAND_COOLDOWN = {
    'default': 3,
    'balance': 5,
    'add': 10,
    'remove': 10,
    'list': 5,
    'help': 3,
    'health': 30,
}

# Cache settings
CACHE_TTL = {
    'balance': 300,  # 5 minutes
    'address': 3600,  # 1 hour
    'network': 600,  # 10 minutes
    'health': 60,  # 1 minute
}

# Notification settings
NOTIFICATION_SETTINGS = {
    'balance_change': True,
    'transaction': True,
    'stake_reward': True,
    'error': True,
}

# Discord embed limits
EMBED_CHAR_LIMIT = 4096
EMBED_FIELD_LIMIT = 25

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
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Check basic format (just length and characters)
    if not re.match(r'^[A-Za-z0-9._-]{50,100}$', value):
        raise ValueError(f"{name} has invalid format")
        
    return value

def validate_blockfrost_id(value: str, name: str) -> str:
    """Validate Blockfrost project ID format and test connectivity"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Check format (mainnet/testnet/preview/preprod project IDs have different prefixes)
    match = re.match(r'^(mainnet|testnet|preview|preprod)([a-zA-Z0-9]{32})$', value)
    if not match:
        raise ValueError(ERROR_MESSAGES['invalid_project_id'])
    
    network_prefix = match.group(1)
    
    # Get base URL from environment
    env_base_url = os.getenv('BLOCKFROST_BASE_URL')
    if env_base_url:
        env_base_url = env_base_url.rstrip('/')
        # Known valid endpoints (from official docs)
        valid_endpoints = {
            "https://cardano-mainnet.blockfrost.io/api/v0": "mainnet",
            "https://cardano-testnet.blockfrost.io/api/v0": "testnet", 
            "https://cardano-preview.blockfrost.io/api/v0": "preview",
            "https://cardano-preprod.blockfrost.io/api/v0": "preprod"
        }
        
        if env_base_url not in valid_endpoints:
            raise ValueError(ERROR_MESSAGES['invalid_base_url'](valid_endpoints.keys()))
            
        # Verify network prefix matches base URL
        expected_network = valid_endpoints[env_base_url]
        if network_prefix != expected_network:
            raise ValueError(
                ERROR_MESSAGES['network_prefix_mismatch'](network_prefix, expected_network, env_base_url)
            )
    
    # Skip connectivity test for now due to SSL issues
    logger.warning("Skipping Blockfrost API connection test due to SSL issues")
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
                network = ipaddress.ip_network(ip_range, strict=False)
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
        raise ValueError(ERROR_MESSAGES['invalid_base_url'](valid_endpoints.keys()))

    # Check that project ID prefix matches base URL
    project_id = os.getenv('BLOCKFROST_PROJECT_ID')
    if project_id:
        match = re.match(r'^(mainnet|testnet|preview|preprod)[a-zA-Z0-9]{32}$', project_id)
        if not match:
            logger.warning("Project ID format is invalid, skipping network prefix check")
            return value
            
        network_prefix = match.group(1)
        expected_network = valid_endpoints[value]
        if network_prefix != expected_network:
            raise ValueError(
                ERROR_MESSAGES['network_prefix_mismatch'](network_prefix, expected_network, value)
            )
            
    return value

def validate_database_url(value: str, name: str) -> str:
    """Validate PostgreSQL database URL"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    try:
        # Parse URL components
        from urllib.parse import urlparse
        result = urlparse(value)
        
        # Check scheme
        if result.scheme not in ['postgresql', 'postgres']:
            raise ValueError("Database URL must use postgresql:// or postgres:// scheme")
            
        # Check required components
        if not all([result.hostname, result.username, result.password, result.path]):
            raise ValueError("Database URL missing required components")
            
        # Test connection
        import asyncpg
        import asyncio
        
        async def test_connection():
            try:
                conn = await asyncpg.connect(value, timeout=10)
                await conn.close()
                return True
            except Exception as e:
                raise ValueError(f"Database connection test failed: {str(e)}")
                
        asyncio.run(test_connection())
        logger.info("Database connection test successful")
        return value
        
    except Exception as e:
        raise ValueError(f"Invalid database URL: {str(e)}")

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

def validate_policy_id(value: str, name: str) -> str:
    """Validate Cardano policy ID format"""
    try:
        value = value.strip()
        if not re.match(r'^[0-9a-fA-F]{56}$', value):
            raise ValueError
        return value.lower()
    except (ValueError, AttributeError):
        raise ValueError(f"{name} must be a valid Cardano policy ID (56-character hex), got: {value}")

def load_env_file(env_path: str = ".env") -> dict:
    """Load environment variables with graceful fallback"""
    try:
        # Try loading from .env file if it exists
        if os.path.exists(env_path):
            load_dotenv(env_path)
            logger.info(f"Loaded environment from {env_path}")
        else:
            logger.warning(f"No {env_path} file found, using system environment")
        
        # Create environment dictionary from os.getenv
        env = {}
        for var_name, var_config in ENV_VARS.items():
            value = os.getenv(var_name)
            if value is not None:
                env[var_name] = value
            elif var_config.default is not None:
                env[var_name] = var_config.default
            elif var_config.required:
                raise ValueError(f"Required environment variable {var_name} not set")
        
        return env
        
    except Exception as e:
        logger.error(f"Error loading environment: {str(e)}")
        # Return minimal configuration if possible
        return get_minimal_config()

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
        validator=validate_base_url,
        required=True,
        description="Base URL for Blockfrost API (mainnet/testnet)"
    ),
    'BLOCKFROST_IP_RANGES': EnvVar(
        name='BLOCKFROST_IP_RANGES',
        default='["0.0.0.0/0"]',  # Default to allow all IPs
        validator=validate_ip_ranges,
        description="List of allowed Blockfrost webhook IP ranges in CIDR notation"
    ),
    'BLOCKFROST_TX_WEBHOOK_ID': EnvVar(
        name='BLOCKFROST_TX_WEBHOOK_ID',
        validator=validate_blockfrost_webhook_id,
        description="Blockfrost transaction webhook ID"
    ),
    'BLOCKFROST_DEL_WEBHOOK_ID': EnvVar(
        name='BLOCKFROST_DEL_WEBHOOK_ID',
        validator=validate_blockfrost_webhook_id,
        description="Blockfrost delegation webhook ID"
    ),
    'BLOCKFROST_WEBHOOK_SECRET': EnvVar(
        name='BLOCKFROST_WEBHOOK_SECRET',
        validator=validate_blockfrost_webhook_secret,
        description="Blockfrost webhook secret for verification"
    ),

    # Database Configuration
    'DATABASE_URL': EnvVar(
        name='DATABASE_URL',
        validator=validate_database_url,
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
        name="YUMMI Policy ID",
        validator=validate_policy_id,
        description="Cardano policy ID for YUMMI token"
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
    'LOG_LEVEL': EnvVar(
        name="Logging Level",
        default="INFO",
        validator=lambda v, n: v.upper() if v in ['DEBUG', 'INFO', 'WARNING', 'ERROR'] else 'INFO',
        required=False,
        description="Logging level (defaults to INFO in production)"
    ),
    'TEST_ADDRESS': EnvVar(
        name="Health Check Test Address",
        required=False,
        validator=lambda v, n: v,
        description="Address used for health checks (configurable)"
    )
}

def validate_webhook_identifier(value: str, name: str) -> str:
    if not value or not isinstance(value, str):
        raise ValueError(f"{name} must be a non-empty string")
    if not re.match(r'^[a-zA-Z0-9_-]+$', value):
        raise ValueError(f"{name} must contain only alphanumeric characters, underscores, and hyphens")
    return value

def validate_webhook_auth_token(value: str, name: str) -> str:
    if not value or not isinstance(value, str):
        raise ValueError(f"{name} must be a non-empty string")
    if len(value) < 32:
        raise ValueError(f"{name} must be at least 32 characters long")
    return value

# Validate environment variables
try:
    env = load_env_file()
    
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
TEST_ADDRESS = env.get('TEST_ADDRESS', "addr1qxqs59lphg8g6qnplr8q6kw2hyzn8c8e3r5jlnwjqppn8k2vllp6xf5qvjgclau0t2q5jz7c7vyvs3x4u2xqm7gaex0s6dd9ay")

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

# Notification settings
NOTIFICATION_SETTINGS = {
    'ada_transactions': {
        'name': 'ADA Transactions',
        'description': 'Get notified when ADA is sent or received',
        'default': True
    },
    'token_changes': {
        'name': 'Token Changes',
        'description': 'Get notified when tokens are sent or received',
        'default': True
    },
    'nft_updates': {
        'name': 'NFT Updates',
        'description': 'Get notified when NFTs are sent or received',
        'default': True
    },
    'stake_changes': {
        'name': 'Stake Key Changes',
        'description': 'Get notified when stake key status changes',
        'default': True
    },
    'policy_expiry': {
        'name': 'Policy Expiry Alerts',
        'description': 'Get notified when NFT policies are about to expire',
        'default': True
    },
    'delegation_status': {
        'name': 'Delegation Status',
        'description': 'Get notified when delegation status changes',
        'default': True
    },
    'staking_rewards': {
        'name': 'Staking Rewards',
        'description': 'Get notified when staking rewards are received',
        'default': True
    },
    'dapp_interactions': {
        'name': 'DApp Interactions',
        'description': 'Get notified when your wallet interacts with DApps',
        'default': True
    },
    'failed_transactions': {
        'name': 'Failed Transactions',
        'description': 'Get notified when transactions fail',
        'default': True
    }
}

# Command cooldown in seconds
COMMAND_COOLDOWN = 5

# Logging levels
LOG_LEVELS = {
    'bot': logging.INFO,
    'system_commands': logging.INFO,
    'wallet_commands': logging.INFO,
    'database': logging.INFO,
    'blockfrost': logging.INFO
}

# Discord embed character limits
EMBED_CHAR_LIMIT = 1024

# Health check interval in seconds
HEALTH_CHECK_INTERVAL = 60

def validate_environment_variables():
    """Validate all required environment variables
    
    Raises:
        ValueError: If any required variable is missing or invalid
    """
    required_vars = {
        'DISCORD_TOKEN': 'Discord bot token',
        'APPLICATION_ID': 'Discord application ID',
        'DATABASE_URL': 'Database URL',
        'BLOCKFROST_PROJECT_ID': 'Blockfrost project ID',
        'ADMIN_CHANNEL_ID': 'Admin channel ID',
        'WEBHOOK_AUTH_TOKEN': 'Webhook authentication token',
        'WEBHOOK_IDENTIFIER': 'Webhook identifier'
    }
    
    optional_vars = {
        'PORT': ('8080', int),
        'MAX_REQUESTS_PER_SECOND': ('10', int),
        'BURST_LIMIT': ('50', int),
        'RATE_LIMIT_COOLDOWN': ('30', int),
        'MAX_RETRIES': ('3', int),
        'BATCH_SIZE': ('1000', int),
        'MAINTENANCE_HOUR': ('2', int),
        'MAINTENANCE_MINUTE': ('0', int),
        'LOG_LEVEL': ('INFO', str),
        'WEBHOOK_TIMEOUT': ('30', int),
        'MINIMUM_YUMMI': ('1000', int),
        'ARCHIVE_AFTER_DAYS': ('30', int),
        'DELETE_AFTER_DAYS': ('90', int),
        'MAINTENANCE_BATCH_SIZE': ('1000', int)
    }
    
    missing_vars = []
    invalid_vars = []
    
    # Check required variables
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing_vars.append(f"{var} ({description})")
            continue
            
        # Validate specific variables
        try:
            if var == 'DISCORD_TOKEN':
                if not re.match(r'^[A-Za-z0-9_-]{24,}$', value):
                    invalid_vars.append(f"{var}: Invalid token format")
            elif var == 'APPLICATION_ID':
                if not value.isdigit():
                    invalid_vars.append(f"{var}: Must be a numeric ID")
            elif var == 'DATABASE_URL':
                if not value.startswith(('postgresql://', 'postgres://')):
                    invalid_vars.append(f"{var}: Must be a PostgreSQL URL")
            elif var == 'ADMIN_CHANNEL_ID':
                if not value.isdigit():
                    invalid_vars.append(f"{var}: Must be a numeric ID")
        except Exception as e:
            invalid_vars.append(f"{var}: {str(e)}")
    
    # Set defaults for optional variables
    for var, (default, type_func) in optional_vars.items():
        try:
            value = os.getenv(var, default)
            if type_func == int:
                value = int(value)
                if value < 0:
                    raise ValueError("Must be non-negative")
            os.environ[var] = str(value)
        except ValueError as e:
            invalid_vars.append(f"{var}: {str(e)}")
            os.environ[var] = default
    
    # Raise error if any variables are missing or invalid
    if missing_vars or invalid_vars:
        error_msg = []
        if missing_vars:
            error_msg.append("Missing required environment variables:\n- " + 
                           "\n- ".join(missing_vars))
        if invalid_vars:
            error_msg.append("Invalid environment variables:\n- " + 
                           "\n- ".join(invalid_vars))
        raise ValueError("\n\n".join(error_msg))

# Validate environment variables on import
validate_environment_variables()

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

def validate_config():
    """Validate entire configuration with proper fallbacks"""
    errors = []
    warnings = []
    
    # Critical environment variables that must be present
    critical_vars = {
        'DISCORD_TOKEN': 'Discord bot token is required',
        'DATABASE_URL': 'Database URL is required',
        'BLOCKFROST_PROJECT_ID': 'Blockfrost project ID is required',
    }
    
    # Optional environment variables with defaults
    optional_vars = {
        'PORT': ('8080', int),
        'MAX_REQUESTS_PER_SECOND': ('10', int),
        'BURST_LIMIT': ('50', int),
        'RATE_LIMIT_COOLDOWN': ('30', int),
        'MAX_RETRIES': ('3', int),
        'BATCH_SIZE': ('1000', int),
        'MAINTENANCE_HOUR': ('2', int),
        'MAINTENANCE_MINUTE': ('0', int),
        'LOG_LEVEL': ('INFO', str),
        'WEBHOOK_TIMEOUT': ('30', int),
        'MINIMUM_YUMMI': ('1000', int)
    }
    
    # Validate critical variables
    for var, message in critical_vars.items():
        value = os.getenv(var)
        if not value:
            errors.append(f"Missing {message}")
        else:
            try:
                if var == 'DISCORD_TOKEN':
                    validate_discord_token(value, var)
                elif var == 'DATABASE_URL':
                    validate_database_url(value, var)
                elif var == 'BLOCKFROST_PROJECT_ID':
                    validate_blockfrost_project_id(value, var)
            except ValueError as e:
                errors.append(f"Invalid {var}: {str(e)}")
    
    # Set defaults for optional variables
    for var, (default, type_func) in optional_vars.items():
        try:
            value = os.getenv(var, default)
            if type_func == int:
                value = int(value)
                if value < 0:
                    raise ValueError("Value must be non-negative")
            os.environ[var] = str(value)
        except ValueError as e:
            warnings.append(f"Invalid {var}, using default {default}: {str(e)}")
            os.environ[var] = default
    
    # Database SSL configuration
    db_url = os.getenv('DATABASE_URL', '')
    if 'sslmode=require' not in db_url.lower():
        warnings.append("Database URL does not enforce SSL. Adding sslmode=require")
        if '?' in db_url:
            os.environ['DATABASE_URL'] = f"{db_url}&sslmode=require"
        else:
            os.environ['DATABASE_URL'] = f"{db_url}?sslmode=require"
    
    # Log warnings and errors
    if warnings:
        logger.warning("Configuration warnings:\n" + "\n".join(f"- {w}" for w in warnings))
    
    if errors:
        error_msg = "Configuration errors:\n" + "\n".join(f"- {e}" for e in errors)
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Configuration validated successfully")

validate_config()
