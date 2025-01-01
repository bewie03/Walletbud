import os
import logging
import re
from typing import Optional, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv
import json
import math
import ipaddress
import aiohttp
import asyncio
import asyncpg
from urllib.parse import urlparse

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
    """Validate Discord token format and test connection"""
    if not value:
        raise ValueError(f"{name} cannot be empty")
        
    # Check basic format
    if not re.match(r'^[A-Za-z0-9._-]{59,}$', value):
        raise ValueError(f"{name} has invalid format")
        
    # Test Discord API connection
    try:
        import aiohttp
        import asyncio
        
        async def test_connection():
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bot {value}"}
                url = "https://discord.com/api/v10/users/@me"
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        raise ValueError(f"Discord API test failed with status {response.status}")
                    return True
                    
        asyncio.run(test_connection())
        logger.info("Discord API connection test successful")
        return value
        
    except Exception as e:
        raise ValueError(f"Failed to validate Discord token: {str(e)}")

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

def validate_ip_ranges(value: str, name: str) -> List[str]:
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
        # Try loading from .env file
        if os.path.exists(env_path):
            load_dotenv(env_path)
            logger.info(f"Loaded environment from {env_path}")
        else:
            logger.warning(f"No {env_path} file found, using system environment")
            
        # Validate environment
        return validate_env_vars()
        
    except Exception as e:
        logger.error(f"Error loading environment: {str(e)}")
        # Return minimal configuration if possible
        return get_minimal_config()

def get_minimal_config() -> dict:
    """Get minimal configuration for basic functionality"""
    minimal_vars = {
        'LOG_LEVEL': 'WARNING',  # Conservative logging
        'MAX_WEBHOOK_SIZE': 1048576,  # 1MB default
        'RATE_LIMIT_COOLDOWN': 60,
        'MAX_REQUESTS_PER_SECOND': 10,
        'BURST_LIMIT': 500
    }
    
    logger.warning("Using minimal configuration - some features will be disabled")
    return minimal_vars

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

# Error messages
ERROR_MESSAGES = {
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Bot API connection is not available. Please try again later.",
    'invalid_address': "Invalid Cardano wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found on the blockchain. Please check the address and try again.",
    'invalid_project_id': "Invalid Blockfrost project ID format. Must start with mainnet/testnet/preview/preprod followed by 32 alphanumeric characters.",
    'invalid_base_url': lambda valid_urls: f"Invalid Blockfrost base URL. Must be one of: {', '.join(valid_urls)}",
    'network_prefix_mismatch': lambda prefix, expected, url: f"Project ID network prefix '{prefix}' does not match base URL network '{expected}' ({url})",
}

def init_blockfrost(self) -> None:
    """Initialize Blockfrost API client with proper error handling"""
    try:
        project_id = os.getenv('BLOCKFROST_PROJECT_ID')
        if not project_id:
            logger.warning("No Blockfrost project ID found, skipping initialization")
            return None
            
        base_url = os.getenv('BLOCKFROST_BASE_URL', '').rstrip('/')
        if not base_url:
            base_url = "https://cardano-mainnet.blockfrost.io/api/v0"
            
        # Initialize Blockfrost API client
        self.blockfrost = BlockFrostApi(
            project_id=project_id,
            base_url=base_url
        )
        logger.info("Blockfrost API client initialized")
        return self.blockfrost
        
    except Exception as e:
        logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
        return None

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
