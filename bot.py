import os
import logging
import sys
from datetime import datetime
import time
import asyncio
import discord
from discord import app_commands
from discord.ext import commands, tasks
import hmac
import hashlib
from aiohttp import web
from blockfrost import BlockFrostApi
from config import *
from db import *

import uuid
import random
import psutil
import functools
from functools import wraps

import config

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

from config import (
    BLOCKFROST_PROJECT_ID,
    MAX_REQUESTS_PER_SECOND,
    BURST_LIMIT,
    RATE_LIMIT_COOLDOWN,
    YUMMI_POLICY_ID,
    YUMMI_TOKEN_NAME,
    MINIMUM_YUMMI,
    WALLET_CHECK_INTERVAL,
    WEBHOOK_IDENTIFIER,
    WEBHOOK_AUTH_TOKEN,
    WEBHOOK_CONFIRMATIONS,
    ASSET_ID
)

from database import (
    get_pool,
    add_wallet_for_user,
    remove_wallet,
    get_user_id_for_wallet,
    get_all_wallets_for_user,
    get_notification_settings,
    update_notification_setting,
    initialize_notification_settings,
    get_wallet_for_user,
    init_db,
    get_all_monitored_addresses,
    get_addresses_for_stake,
    update_pool_for_stake,
    cleanup_pool  # Import cleanup_pool function
)

def get_request_id():
    """Generate a unique request ID for logging"""
    return str(uuid.uuid4())

def dm_only():
    """Decorator to restrict commands to DMs only"""
    async def predicate(interaction: discord.Interaction):
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(
                "❌ This command can only be used in DMs for security reasons.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

def has_blockfrost(func=None):
    """Decorator to check if Blockfrost client is available"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            if not self.blockfrost_client:
                await interaction.response.send_message(
                    "❌ This command is currently unavailable due to API connectivity issues. Please try again later.",
                    ephemeral=True
                )
                return
            return await func(self, interaction, *args, **kwargs)
        return wrapper
    return decorator(func) if func else decorator

class RateLimiter:
    """Rate limiter that implements Blockfrost's rate limiting rules:
    - 10 requests per second base rate
    - Burst of 500 requests allowed
    - Burst cools off at 10 requests per second
    """
    def __init__(self, max_requests: int = MAX_REQUESTS_PER_SECOND, 
                 burst_limit: int = BURST_LIMIT, 
                 cooldown_seconds: int = RATE_LIMIT_COOLDOWN):
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if burst_limit < max_requests:
            raise ValueError("burst_limit must be >= max_requests")
        if cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be positive")
            
        self.max_requests = max_requests
        self.burst_limit = burst_limit
        self.cooldown_seconds = cooldown_seconds
        self.endpoint_locks = {}  # Locks per endpoint
        self.endpoint_requests = {}  # Request tracking per endpoint
        self.endpoint_burst_tokens = {}  # Burst tokens per endpoint
        self.endpoint_last_update = {}  # Last update time per endpoint
        self.lock = asyncio.Lock()  # Global lock for endpoint management
        
        logger.info(
            f"Initialized RateLimiter with: max_requests={max_requests}, "
            f"burst_limit={burst_limit}, cooldown_seconds={cooldown_seconds}"
        )
        
    async def get_endpoint_lock(self, endpoint: str) -> asyncio.Lock:
        """Get or create a lock for a specific endpoint"""
        async with self.lock:
            if endpoint not in self.endpoint_locks:
                self.endpoint_locks[endpoint] = asyncio.Lock()
                self.endpoint_requests[endpoint] = []
                self.endpoint_burst_tokens[endpoint] = self.burst_limit
                self.endpoint_last_update[endpoint] = time.time()
            return self.endpoint_locks[endpoint]
            
    async def acquire(self, endpoint: str):
        """Acquire a rate limit token for a specific endpoint"""
        lock = await self.get_endpoint_lock(endpoint)
        async with lock:
            now = time.time()
            
            # Update burst tokens
            time_passed = now - self.endpoint_last_update[endpoint]
            tokens_to_add = int(time_passed * self.max_requests)
            self.endpoint_burst_tokens[endpoint] = min(
                self.burst_limit, 
                self.endpoint_burst_tokens[endpoint] + tokens_to_add
            )
            self.endpoint_last_update[endpoint] = now
            
            # Clean old requests
            cutoff = now - self.cooldown_seconds
            self.endpoint_requests[endpoint] = [
                t for t in self.endpoint_requests[endpoint] if t > cutoff
            ]
            
            # Check if we can make a request
            if (len(self.endpoint_requests[endpoint]) >= self.max_requests and 
                self.endpoint_burst_tokens[endpoint] <= 0):
                wait_time = self.endpoint_requests[endpoint][0] - cutoff
                logger.warning(
                    f"Rate limit hit for endpoint {endpoint}, "
                    f"waiting {wait_time:.2f}s"
                )
                await asyncio.sleep(wait_time)
                return await self.acquire(endpoint)
            
            # Use burst token if needed
            if len(self.endpoint_requests[endpoint]) >= self.max_requests:
                self.endpoint_burst_tokens[endpoint] -= 1
                
            # Record request
            self.endpoint_requests[endpoint].append(now)
            logger.debug(
                f"Rate limit request granted for {endpoint} "
                f"(burst tokens: {self.endpoint_burst_tokens[endpoint]})"
            )
            
    async def release(self, endpoint: str):
        """Release the rate limit token for a specific endpoint"""
        lock = await self.get_endpoint_lock(endpoint)
        async with lock:
            if self.endpoint_requests[endpoint]:
                self.endpoint_requests[endpoint].pop()

    async def __aenter__(self):
        """Enter the async context manager"""
        await self.acquire("default")  # Use default endpoint if none specified
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager"""
        await self.release("default")

class WalletBud(commands.Bot):
    """WalletBud Discord bot"""
    NOTIFICATION_TYPES = {
        # Command value -> Database key
        "ada_transactions": "ada_transactions",
        "token_changes": "token_changes", 
        "nft_updates": "nft_updates",
        "stake_changes": "stake_changes",
        "policy_expiry": "policy_expiry",
        "delegation_status": "delegation_status",
        "staking_rewards": "staking_rewards",
        "dapp_interactions": "dapp_interactions",
        "failed_transactions": "failed_transactions"
    }
    
    NOTIFICATION_DISPLAY = {
        # Database key -> Display name
        "ada_transactions": "ADA Transactions",
        "token_changes": "Token Changes",
        "nft_updates": "NFT Updates",
        "stake_changes": "Stake Key Changes",
        "policy_expiry": "Policy Expiry Alerts",
        "delegation_status": "Delegation Status",
        "staking_rewards": "Staking Rewards",
        "dapp_interactions": "DApp Interactions",
        "failed_transactions": "Failed Transactions"
    }

    DEFAULT_SETTINGS = {
        "ada_transactions": True,
        "token_changes": True,
        "nft_updates": True,
        "stake_changes": True,
        "policy_expiry": True,
        "delegation_status": True,
        "staking_rewards": True,
        "dapp_interactions": True,
        "failed_transactions": True
    }

    DAPP_IDENTIFIERS = {
        # Common metadata fields
        "metadata_fields": ["dapp", "platform", "protocol", "application"],
        
        # DApp name -> List of identifiers in metadata
        "SundaeSwap": ["sundae", "sundaeswap", "sundaeswap.io", "swap.sundae"],
        "MuesliSwap": ["muesli", "muesliswap", "muesliswap.com", "swap.muesli"],
        "MinSwap": ["min", "minswap", "minswap.org", "swap.min"],
        "WingRiders": ["wing", "wingriders", "wingriders.com"],
        "VyFinance": ["vyfi", "vyfinance", "vyfi.io"],
        "Genius Yield": ["genius", "geniusyield", "geniusyield.co"],
        "Indigo Protocol": ["indigo", "indigoprotocol", "indigoprotocol.io"],
        "Liqwid Finance": ["liqwid", "liqwidfinance", "liqwid.finance"],
        "AADA Finance": ["aada", "aadafinance", "aada.finance"],
        "Djed": ["djed", "shen", "djed.xyz"],
        "Ardana": ["ardana", "ardana.io"],
        "Optim Finance": ["optim", "optimfinance"],
        "Spectrum": ["spectrum", "spectrum.fi"],
        "JPEG Store": ["jpeg", "jpgstore", "jpg.store"],
        "NFT/Token Contract": ["721", "20", "nft", "token"]  # NFT and Token standards
    }

    def __init__(self):
        """Initialize the bot"""
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None
        )
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests=MAX_REQUESTS_PER_SECOND, 
            burst_limit=BURST_LIMIT, 
            cooldown_seconds=RATE_LIMIT_COOLDOWN
        )
        logger.info(f"Initialized RateLimiter with: max_requests={MAX_REQUESTS_PER_SECOND}, burst_limit={BURST_LIMIT}, cooldown_seconds={RATE_LIMIT_COOLDOWN}")
        
        # Admin channel for error notifications
        try:
            self.admin_channel_id = os.getenv('ADMIN_CHANNEL_ID')
            if self.admin_channel_id:
                logger.info(f"Admin channel configured: {self.admin_channel_id}")
            else:
                logger.warning("No admin channel configured (ADMIN_CHANNEL_ID not set)")
        except Exception as e:
            logger.error(f"Error configuring admin channel: {str(e)}")
            self.admin_channel_id = None
        
        # Initialize Blockfrost client
        self.blockfrost_client = None
        
        # Initialize monitoring state
        self.monitoring_paused = False
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
        # Pre-compile DApp metadata patterns
        self.dapp_patterns = {
            name.lower(): [pattern.lower() for pattern in patterns]
            for name, patterns in self.DAPP_IDENTIFIERS.items()
            if name != "metadata_fields"
        }
        self.metadata_fields = [field.lower() for field in self.DAPP_IDENTIFIERS["metadata_fields"]]
        
        # Initialize webhook server
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)
        self.port = int(os.getenv('PORT', 49152))  # Use Heroku's PORT or fallback to 49152
        self.monitored_addresses = set()
        self.monitored_stake_addresses = set()
        
        # Add start time for uptime tracking
        self.start_time = datetime.now()
        
        # Initialize interaction rate limiting
        self.interaction_cooldowns = {}
        self.interaction_lock = asyncio.Lock()
        self.active_interactions = {}
        self.interaction_timeouts = {}
        self.webhook_retries = {}
        
        # Initialize wallet monitoring task
        self.monitor_wallets = tasks.loop(seconds=60)(self._monitor_wallets)  # Run every minute

    async def setup_hook(self):
        """Setup hook called before the bot starts"""
        try:
            logger.info("Starting setup hook...")
            
            # Initialize database
            logger.info("Initializing database...")
            await init_db()
            logger.info("Database initialized")
            
            # Initialize Blockfrost client
            logger.info("Initializing Blockfrost client...")
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost client")
                return
            logger.info("Blockfrost client initialized")
            
            # Load monitored addresses
            logger.info("Loading monitored addresses...")
            addresses, stake_addresses = await get_all_monitored_addresses()
            self.monitored_addresses = addresses
            self.monitored_stake_addresses = stake_addresses
            
            logger.info(f"Loaded {len(addresses)} monitored addresses")
            logger.info(f"Loaded {len(stake_addresses)} stake addresses")
            
            # Set up webhook routes with retry mechanism
            self.app.router.add_post('/webhooks/blockfrost', self.handle_webhook_with_retry)
            
            # Start webhook server
            await self.runner.setup()
            site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await site.start()
            logger.info(f"Webhook server started on port {self.port}")
            
            # Start monitoring task
            self.monitor_wallets.start()
            logger.info("Started wallet monitoring task")
            
            logger.info("Setup complete")
            
        except Exception as e:
            logger.error(f"Error in setup: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise e
            
    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            # Get project ID and base URL from environment and strip quotes
            project_id = os.getenv('BLOCKFROST_PROJECT_ID', '').strip('"\'')
            base_url = os.getenv('BLOCKFROST_BASE_URL', 'https://cardano-mainnet.blockfrost.io/api/v0').strip('"\'')
            
            logger.info(f"Attempting to initialize Blockfrost with:")
            logger.info(f"Project ID: {project_id[:8]}...")
            logger.info(f"Base URL: {base_url}")
            
            if not project_id:
                logger.error("BLOCKFROST_PROJECT_ID not set in environment variables")
                return False

            logger.info(f"Creating Blockfrost client with project ID: {project_id[:8]}...")
            self.blockfrost_client = BlockFrostApi(
                project_id=project_id,
                base_url=base_url
            )
            logger.info("Blockfrost client created successfully")
            
            # Test connection with health endpoint
            logger.info("Testing Blockfrost connection...")
            try:
                health = await self.rate_limited_request(self.blockfrost_client.health)
                logger.info(f"Health check response: {health}")
                
                # Also get network info as additional verification
                network = await self.rate_limited_request(self.blockfrost_client.network)
                logger.info(f"Network info: {network}")
                
                logger.info("✅ Blockfrost connection test successful")
                return True
                
            except Exception as e:
                logger.error(f"❌ Connection test failed: {str(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                if "Invalid project token" in str(e):
                    logger.error("Invalid Blockfrost API key. Please check your BLOCKFROST_PROJECT_ID")
                elif "Forbidden" in str(e):
                    logger.error("API key doesn't have required permissions")
                else:
                    logger.error(f"Error details: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error initializing Blockfrost client: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return False

    async def rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited request to the Blockfrost API with retry logic"""
        max_retries = 3
        base_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                # Check if func is a coroutine or regular function
                is_coroutine = asyncio.iscoroutinefunction(func)
                logger.debug(f"Making API request: {func.__name__} (is_coroutine={is_coroutine})")
                
                async with self.rate_limiter:
                    if is_coroutine:
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                    logger.debug(f"API request successful: {func.__name__}")
                    return result
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Log full error details
                logger.error(f"API request failed: {func.__name__}")
                logger.error(f"Error type: {error_type}")
                logger.error(f"Error message: {error_msg}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                
                # Specific error handling
                if "rate limit" in error_msg.lower():
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Rate limit hit, retrying in {delay}s (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                    
                elif "not found" in error_msg.lower():
                    logger.error(f"Resource not found: {error_msg}")
                    raise
                    
                elif "unauthorized" in error_msg.lower():
                    logger.error("API authentication failed. Check BLOCKFROST_PROJECT_ID")
                    await self.send_admin_alert("⚠️ Blockfrost API authentication failed")
                    raise
                    
                elif "timeout" in error_msg.lower():
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Request timeout, retrying in {delay}s (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Request timed out after all retries")
                        raise
                        
                else:
                    logger.error(f"Unexpected error ({error_type}): {error_msg}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        await self.send_admin_alert(f"❌ API Error: {error_type} - {error_msg}")
                        raise
        
        raise Exception(f"Failed after {max_retries} attempts")

    async def send_admin_alert(self, message: str, is_error: bool = True):
        """Send alert to admin channel with enhanced details"""
        try:
            if not self.admin_channel_id:
                return
                
            channel = self.get_channel(int(self.admin_channel_id))
            if not channel:
                logger.error("Admin channel not found")
                return
                
            # Create rich embed for alert
            embed = discord.Embed(
                title="🚨 Error Alert" if is_error else "ℹ️ System Alert",
                description=message,
                color=discord.Color.red() if is_error else discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Add system info
            embed.add_field(
                name="System Status",
                value=f"""
                • Database: {'✅' if await self.check_database() else '❌'}
                • Blockfrost: {'✅' if await self.check_blockfrost() else '❌'}
                • Memory Usage: {psutil.Process().memory_info().rss / 1024 / 1024:.1f} MB
                • CPU Usage: {psutil.cpu_percent()}%
                """.strip(),
                inline=False
            )
            
            # Add error context if available
            import traceback
            tb = traceback.format_exc()
            if tb and tb != "NoneType: None\n":
                embed.add_field(
                    name="Error Details",
                    value=f"```python\n{tb[:1000]}```",
                    inline=False
                )
            
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to send admin alert: {str(e)}")

    async def check_yummi_requirement(self, address: str, user_id: int):
        """Check if wallet meets YUMMI token requirement"""
        try:
            utxos = await self.rate_limited_request(
                self.blockfrost_client.address_utxos_asset,
                address=address,
                asset=ASSET_ID
            )
            
            yummi_balance = sum(
                int(amount.quantity)
                for utxo in utxos
                for amount in utxo.amount
                if amount.unit.lower() == ASSET_ID.lower()
            )
            
            if yummi_balance < MINIMUM_YUMMI:
                await self.notify_user(
                    user_id,
                    f"❌ Wallet {address[:8]}... has been removed due to insufficient YUMMI balance"
                )
                await remove_wallet(address)
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking YUMMI requirement: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return False

    async def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if we should send a notification to the user"""
        try:
            # Get database connection
            pool = await get_pool()
            if not pool:
                logger.error("Failed to get database pool")
                return False
                
            # Use database should_notify function
            return await should_notify(str(user_id), notification_type)
            
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            return False  # Default to not notifying on error

    async def process_interaction(self, interaction: discord.Interaction, ephemeral: bool = True):
        """Process interaction with proper error handling"""
        try:
            # Defer response immediately
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except discord.errors.NotFound:
            logger.error("Interaction not found")
            return False
        except Exception as e:
            logger.error(f"Error deferring interaction: {str(e)}")
            return False

    async def safe_send(self, interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> bool:
        """Safely send a message through interaction, handling rate limits and errors"""
        interaction_id = str(interaction.id)
        
        # Check if we've already responded to this interaction
        if interaction_id in self.active_interactions:
            logger.debug(f"Already responded to interaction {interaction_id}")
            return False
        
        # Mark interaction as active
        self.active_interactions[interaction_id] = True
        
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content, ephemeral=ephemeral)
            return True
        except discord.errors.InteractionResponded:
            logger.debug(f"Interaction {interaction_id} already responded to")
            return False
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limited
                logger.warning(f"Rate limited on interaction {interaction_id}")
                # Clear interaction from active list so we can retry
                self.active_interactions.pop(interaction_id, None)
                return False
            logger.error(f"HTTP error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            return False
        finally:
            # Clean up interaction tracking after a delay
            asyncio.create_task(self.cleanup_interaction(interaction_id))

    async def cleanup_interaction(self, interaction_id: str):
        """Clean up completed interaction"""
        await asyncio.sleep(5)  # Wait 5 seconds before cleanup
        self.active_interactions.pop(interaction_id, None)
        self.webhook_retries.pop(interaction_id, None)

    async def send_response(self, interaction: discord.Interaction, content=None, embed=None, ephemeral: bool = True):
        """Send response with proper error handling"""
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
            return True
        except Exception as e:
            logger.error(f"Error sending response: {str(e)}")
            return False

    async def send_dm(self, user_id: int, content: str):
        """Send a direct message to a user"""
        try:
            user = await self.fetch_user(user_id)
            if user:
                await user.send(content)
            else:
                logger.error(f"Failed to find user {user_id} for DM")
        except Exception as e:
            logger.error(f"Error sending DM: {str(e)}")

    async def _notify_token_change(self, address: str, user_id: int, token_id: str, amount: int, change_type: str, change_amount: int = None):
        """Send token change notification"""
        token_name = token_id.encode('utf-8').hex()[-8:]  # Get last 4 bytes of policy ID
        
        if change_type == "new":
            message = (
                f"🪙 **New Token Detected!**\n"
                f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                f"Token: `{token_name}`\n"
                f"Amount: `{amount:,}`"
            )
        elif change_type == "increase":
            message = (
                f"🪙 **Token Balance Increased!**\n"
                f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                f"Token: `{token_name}`\n"
                f"Received: `{change_amount:,}`\n"
                f"New Balance: `{amount:,}`"
            )
        else:  # decrease
            message = (
                f"🪙 **Token Balance Decreased!**\n"
                f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                f"Token: `{token_name}`\n"
                f"Sent: `{change_amount:,}`\n"
                f"New Balance: `{amount:,}`"
            )
            
        await self.send_dm(int(user_id), message)

    async def _add_wallet(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            # Validate address format
            if not address.startswith("addr1"):
                await self.safe_send(interaction, "❌ Invalid address format. Address must start with 'addr1'")
                return

            # Get stake address
            stake_address = None
            try:
                stake_info = await self.rate_limited_request(
                    self.blockfrost_client.address_info, address
                )
                stake_address = stake_info.stake_address
            except Exception as e:
                logger.error(f"Error getting stake address: {str(e)}")

            # Add wallet to database
            success = await add_wallet_for_user(str(interaction.user.id), address, stake_address)
            if not success:
                await self.safe_send(interaction, "❌ Failed to add wallet to monitoring")
                return

            # Update monitored addresses
            self.monitored_addresses.add(address)
            if stake_address:
                self.monitored_stake_addresses.add(stake_address)

            await self.safe_send(interaction, f"✅ Now monitoring wallet: `{address}`")

        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await self.safe_send(interaction, "❌ An error occurred while adding the wallet")

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists
            wallets = await get_all_wallets_for_user(str(interaction.user.id))
            if address not in wallets:
                await self.safe_send(interaction, "Wallet not found in your registered wallets.")
                return
                
            # Reset YUMMI warnings
            await reset_yummi_warning(wallet['id'])
            
            # Remove wallet
            success = await remove_wallet(address)
            if success:
                await self.safe_send(interaction, "✅ Wallet removed successfully!", ephemeral=True)
            else:
                await self.safe_send(interaction, "❌ Failed to remove wallet.", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await self.safe_send(interaction, "❌ An error occurred while removing the wallet", ephemeral=True)

    async def _list_wallets(self, interaction: discord.Interaction):
        """List all registered wallets"""
        try:
            # Get user's wallets
            addresses = await get_all_wallets_for_user(str(interaction.user.id))
            
            # Check if user has any wallets
            if not addresses:
                await self.safe_send(
                    interaction,
                    "❌ You don't have any registered wallets! Use `/addwallet` to add one.",
                    ephemeral=True
                )
                return
            
            # Create embed
            embed = discord.Embed(
                title="📋 Your Registered Wallets",
                description=f"You have {len(addresses)} registered wallet{'s' if len(addresses) != 1 else ''}:",
                color=discord.Color.blue()
            )
            
            # Add field for each wallet
            for i, address in enumerate(addresses, 1):
                try:
                    # Get wallet details
                    wallet = await get_wallet_for_user(str(interaction.user.id), address)
                    if not wallet:
                        logger.error(f"No wallet found for address {address}")
                        continue
                        
                    wallet_id = wallet['id']
                    
                    # Get warning count
                    warning_count = await get_yummi_warning_count(wallet_id)
                    
                    # Add field
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=(
                            f"**Address:** `{address[:8]}...{address[-8:]}`\n"
                            f"**YUMMI Warnings:** {warning_count}/3\n"
                            f"**Full Address:**\n`{address}`"
                        ),
                        inline=False
                    )
                except Exception as e:
                    logger.error(f"Error getting details for wallet {address}: {str(e)}")
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=f"❌ Error retrieving details for `{address[:8]}...{address[-8:]}`",
                        inline=False
                    )
            
            await self.safe_send(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while listing your wallets",
                ephemeral=True
            )

    async def _help(self, interaction: discord.Interaction):
        """Show bot help and commands"""
        try:
            embed = discord.Embed(
                title="🤖 WalletBud Help",
                description="Here are all available commands:",
                color=discord.Color.blue()
            )

            # Core commands
            embed.add_field(
                name="Core Commands",
                value=(
                    "`/help` - Show this help message\n"
                    "`/health` - Check bot and API status\n"
                    "`/stats` - Show system statistics"
                ),
                inline=False
            )

            # Wallet commands
            embed.add_field(
                name="Wallet Management",
                value=(
                    "`/addwallet <address>` - Add a wallet to monitor\n"
                    "`/removewallet <address>` - Remove a monitored wallet\n"
                    "`/list_wallets` - List your registered wallets\n"
                    "`/balance` - Show your wallet balances"
                ),
                inline=False
            )

            # Notification commands
            embed.add_field(
                name="Notification Settings",
                value=(
                    "`/notifications` - View your notification settings\n"
                    "`/toggle <setting> <enabled>` - Toggle a notification type\n\n"
                    "**Available Notification Types:**\n"
                    "• `ada_transactions` - ADA transfers\n"
                    "• `token_changes` - Token balance changes\n"
                    "• `nft_updates` - NFT additions/removals\n"
                    "• `stake_changes` - Stake key registration/deregistration\n"
                    "• `policy_expiry` - NFT policy expiry alerts\n"
                    "• `delegation_status` - Pool delegation changes\n"
                    "• `staking_rewards` - Staking reward deposits\n"
                    "• `dapp_interactions` - DApp transaction detection\n"
                    "• `failed_transactions` - Failed transaction alerts"
                ),
                inline=False
            )

            # Requirements note
            embed.add_field(
                name="Requirements",
                value=(
                    f"• Must hold minimum {MINIMUM_YUMMI:,} YUMMI tokens\n"
                    "• Commands only work in DMs\n"
                    "• Wallets are checked every 6 hours for YUMMI balance"
                ),
                inline=False
            )

            await self.safe_send(interaction, embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error showing help: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while showing help",
                ephemeral=True
            )

    async def _health(self, interaction: discord.Interaction):
        """Check bot and API status"""
        try:
            # Test Blockfrost connection
            api_status = "✅ Online" if self.blockfrost_client else "❌ Offline"
            api_details = ""
            
            if not self.blockfrost_client:
                api_details = "\n❌ Error: Blockfrost API key not configured"
            else:
                try:
                    await self.rate_limited_request(self.blockfrost_client.health)
                    api_details = "\n✅ API connection test successful"
                except Exception as e:
                    api_status = "❌ Error"
                    if "Invalid project token" in str(e):
                        api_details = "\n❌ Invalid API key"
                    elif "Forbidden" in str(e):
                        api_details = "\n❌ API key doesn't have permissions"
                    else:
                        api_details = f"\n❌ API Error: {str(e)}"
            
            # Create embed
            embed = discord.Embed(
                title="🏥 System Health",
                color=discord.Color.blue() if "✅" in api_status else discord.Color.red()
            )
            
            # Add fields
            embed.add_field(
                name="Bot Status",
                value="✅ Online",
                inline=True
            )
            embed.add_field(
                name="API Status",
                value=f"{api_status}{api_details}",
                inline=True
            )
            embed.add_field(
                name="Uptime",
                value=f"`{datetime.now() - self.start_time}`",
                inline=False
            )
            
            await self.safe_send(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error checking health: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while checking system health",
                ephemeral=True
            )

    async def _balance(self, interaction: discord.Interaction):
        """Get your wallet's current balance"""
        try:
            # Get user's wallets
            addresses = await get_all_wallets_for_user(str(interaction.user.id))
            if not addresses:
                await self.safe_send(
                    interaction,
                    "❌ You don't have any registered wallets! Use `/addwallet` to add one.",
                    ephemeral=True
                )
                return
            
            # Create embed
            embed = discord.Embed(
                title="💰 Your Wallet Balances",
                description=f"You have {len(addresses)} registered wallet{'s' if len(addresses) != 1 else ''}:",
                color=discord.Color.blue()
            )
            
            total_ada = 0
            
            # Add field for each wallet
            for i, address in enumerate(addresses, 1):
                try:
                    # Get wallet details
                    wallet = await get_wallet_for_user(str(interaction.user.id), address)
                    if not wallet:
                        logger.error(f"No wallet found for address {address}")
                        continue
                        
                    # Get current UTXOs
                    utxos = await self.rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    # Calculate ADA balance
                    ada_balance = sum(
                        int(utxo.amount[0].quantity)
                        for utxo in utxos 
                        if utxo.amount and utxo.amount[0].unit == 'lovelace'
                    ) / 1_000_000
                    
                    total_ada += ada_balance
                    
                    # Format address for display
                    short_address = f"{address[:8]}...{address[-8:]}"
                    
                    # Add wallet field
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=(
                            f"**Address:** `{short_address}`\n"
                            f"**Balance:** `{ada_balance:,.2f} ADA`\n"
                            f"**Full Address:**\n`{address}`"
                        ),
                        inline=False
                    )
                    
                except Exception as e:
                    logger.error(f"Error getting balance for wallet {address}: {str(e)}")
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=f"❌ Error retrieving balance for `{address[:8]}...{address[-8:]}`",
                        inline=False
                    )
            
            # Add total balance at the top
            embed.description = (
                f"You have {len(addresses)} registered wallet{'s' if len(addresses) != 1 else ''}\n"
                f"**Total Balance:** `{total_ada:,.2f} ADA`"
            )

            await self.safe_send(interaction, embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error getting balances: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while retrieving wallet balances",
                ephemeral=True
            )

    async def _notifications(self, interaction: discord.Interaction):
        """View your notification settings"""
        try:
            # Get user's notification settings
            settings = await get_notification_settings(str(interaction.user.id))
            if not settings:
                await initialize_notification_settings(str(interaction.user.id))
                settings = self.DEFAULT_SETTINGS.copy()
            
            # Create embed
            embed = discord.Embed(
                title="🔔 Notification Settings",
                description="Configure which notifications you receive. Use `/toggle <type>` to change a setting.",
                color=discord.Color.blue()
            )
            
            # Add fields for each notification type
            for db_key, display_name in self.NOTIFICATION_DISPLAY.items():
                status = settings.get(db_key, True)  # Default to True if not set
                emoji = "✅" if status else "❌"
                value = f"{emoji} **{display_name}**\n`/toggle {db_key}` to change"
                embed.add_field(name=display_name, value=value, inline=True)
            
            await self.safe_send(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing notification settings: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while showing your notification settings.",
                ephemeral=True
            )

    async def _toggle(self, interaction: discord.Interaction, setting: str, enabled: bool):
        """Toggle notification settings"""
        try:
            # Update the setting
            if await update_notification_setting(str(interaction.user.id), setting, enabled):
                status = "enabled" if enabled else "disabled"
                await self.safe_send(
                    interaction,
                    f"✅ {self.NOTIFICATION_DISPLAY[setting]} notifications {status}!",
                    ephemeral=True
                )
            else:
                await self.safe_send(
                    interaction,
                    "❌ Failed to update notification settings. Please try again.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error updating notification setting: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while updating your notification settings.",
                ephemeral=True
            )

    async def _stats(self, interaction: discord.Interaction):
        """Show system monitoring statistics"""
        try:
            # Get total wallets and users
            all_wallets = await get_all_wallets()
            if not all_wallets:
                all_wallets = []
            
            # Get unique users and stake addresses
            unique_users = set(wallet['user_id'] for wallet in all_wallets)
            stake_addresses = set(wallet['stake_address'] for wallet in all_wallets if wallet.get('stake_address'))
            
            # Calculate total ADA being monitored
            total_ada = sum(
                float(wallet['ada_balance'] or 0)
                for wallet in all_wallets
            )
            
            # Get system stats
            process = psutil.Process()
            memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
            cpu_percent = process.cpu_percent()
            thread_count = process.num_threads()
            
            # Create embed
            embed = discord.Embed(
                title="📊 System Statistics",
                description="Current system monitoring statistics",
                color=discord.Color.blue()
            )
            
            # Add monitoring stats
            embed.add_field(
                name="Monitoring Stats",
                value=(
                    f"**Total Wallets:** `{len(all_wallets):,}`\n"
                    f"**Unique Users:** `{len(unique_users):,}`\n"
                    f"**Stake Addresses:** `{len(stake_addresses):,}`\n"
                    f"**Total ADA Monitored:** `{total_ada:,.2f} ADA`\n"
                    f"**System Load:** `{len(all_wallets) / 5000 * 100:.1f}%`"  # Based on 5000 wallet safe limit
                ),
                inline=False
            )
            
            # Add system stats
            embed.add_field(
                name="System Stats",
                value=(
                    f"**Memory Usage:** `{memory_usage:.1f} MB`\n"
                    f"**CPU Usage:** `{cpu_percent:.1f}%`\n"
                    f"**Active Threads:** `{thread_count}`\n"
                    f"**Uptime:** `{datetime.now() - self.start_time}`\n"
                    f"**API Status:** `{'✅ Online' if self.blockfrost_client else '❌ Offline'}`"
                ),
                inline=False
            )

            await self.safe_send(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            await self.safe_send(
                interaction,
                "❌ An error occurred while retrieving system statistics",
                ephemeral=True
            )

    async def on_ready(self):
        """Called when the bot is ready"""
        try:
            # Set up commands
            @self.tree.command(name="help", description="Show bot help and commands")
            async def help(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._help(interaction)

            @self.tree.command(name="health", description="Check bot health status")
            async def health(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._health(interaction)

            @self.tree.command(name="balance", description="Get your wallet's current balance")
            async def balance(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._balance(interaction)

            @self.tree.command(name="notifications", description="View your notification settings")
            async def notifications(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._notifications(interaction)

            @self.tree.command(name="addwallet", description="Add a wallet address to monitor")
            @app_commands.describe(address="The wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._add_wallet(interaction, address)

            @self.tree.command(name="removewallet", description="Remove a monitored wallet address")
            @app_commands.describe(address="The wallet address to stop monitoring")
            async def removewallet(interaction: discord.Interaction, address: str):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._remove_wallet(interaction, address)

            @self.tree.command(name="toggle", description="Toggle notification settings")
            @app_commands.describe(
                setting="The notification setting to toggle",
                enabled="Whether to enable or disable the setting"
            )
            async def toggle(interaction: discord.Interaction, setting: str, enabled: bool):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._toggle(interaction, setting, enabled)

            @self.tree.command(name="list", description="List your monitored wallets")
            async def list_wallets(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._list_wallets(interaction)

            @self.tree.command(name="stats", description="Show system monitoring statistics")
            async def stats(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._stats(interaction)
            
            # Sync commands
            try:
                logger.info("Syncing commands...")
                await self.tree.sync()
                logger.info("Bot is ready and commands are synced!")
            except Exception as e:
                logger.error(f"Error syncing commands: {str(e)}")
                raise e
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")
            raise e

    async def close(self):
        """Cleanup and close the bot"""
        logger.info("Shutting down bot...")
        await database.cleanup_pool()  # Add database cleanup
        await super().close()

    async def _monitor_wallets(self):
        """Monitor wallets for changes"""
        try:
            if self.monitoring_paused:
                return
                
            async with self.wallet_task_lock:
                if self.processing_wallets:
                    return
                self.processing_wallets = True
                
            try:
                # Get all monitored addresses
                addresses = list(self.monitored_addresses)
                logger.info(f"Checking {len(addresses)} wallets...")
                
                for address in addresses:
                    try:
                        # Get user ID for this wallet
                        user_id = await get_user_id_for_wallet(address)
                        if not user_id:
                            continue
                            
                        # Get current UTXOs
                        utxos = await self.rate_limited_request(
                            self.blockfrost_client.address_utxos,
                            address
                        )
                        
                        # Get recent transactions
                        txs = await self.rate_limited_request(
                            self.blockfrost_client.address_transactions,
                            address,
                            params={"from": "0", "to": "10"}  # Last 10 transactions
                        )
                        
                        # Process the changes
                        await self._check_balance_changes(address, user_id, utxos, txs)
                        
                    except Exception as e:
                        logger.error(f"Error monitoring wallet {address}: {str(e)}")
                        continue
                        
            finally:
                self.processing_wallets = False
                
        except Exception as e:
            logger.error(f"Error in monitor_wallets task: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

    async def handle_webhook_with_retry(self, request: web.Request, max_retries: int = 3) -> web.Response:
        """Handle webhook with exponential backoff retry"""
        retry_count = 0
        while retry_count < max_retries:
            try:
                return await self.handle_webhook(request)
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    logger.error(f"Final webhook handling attempt failed: {str(e)}")
                    raise
                
                # Exponential backoff: 2^retry_count seconds
                wait_time = 2 ** retry_count
                logger.warning(f"Webhook handling attempt {retry_count} failed. Retrying in {wait_time}s")
                await asyncio.sleep(wait_time)

    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhooks from Blockfrost"""
        try:
            # Get webhook ID and auth token from headers
            webhook_id = request.headers.get('Webhook-Id')
            auth_token = request.headers.get('Auth-Token')
            signature = request.headers.get('Signature')
            
            # Validate webhook ID and auth token
            webhook_config = None
            for config in WEBHOOKS.values():
                if config['id'] == webhook_id and config['auth_token'] == auth_token:
                    webhook_config = config
                    break
                    
            if not webhook_config:
                logger.error(f"Invalid webhook ID or auth token: {webhook_id}")
                return web.Response(status=401, text="Invalid webhook ID or auth token")
            
            # Get request body
            body = await request.text()
            
            # Validate signature
            expected_signature = hmac.new(
                auth_token.encode(),
                body.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if signature != expected_signature:
                logger.error(f"Invalid signature for webhook {webhook_id}")
                return web.Response(status=401, text="Invalid signature")
            
            # Parse payload
            payload = await request.json()
            
            # Validate event type
            event_type = payload.get('event_type')
            if not event_type:
                logger.error("Missing event_type in webhook payload")
                return web.Response(status=400, text="Missing event_type")
                
            if event_type not in WEBHOOKS:
                logger.error(f"Invalid event_type: {event_type}")
                return web.Response(status=400, text="Invalid event_type")
            
            # Check confirmations
            confirmations = payload.get('confirmations', 0)
            if confirmations < webhook_config['confirmations']:
                logger.info(f"Waiting for more confirmations. Current: {confirmations}, Required: {webhook_config['confirmations']}")
                return web.Response(status=202, text="Waiting for more confirmations")
            
            # Process webhook based on event type
            if event_type == 'transaction':
                await self.handle_transaction_webhook(payload)
            elif event_type == 'delegation':
                await self.handle_delegation_webhook(payload)
            
            return web.Response(status=200, text="OK")
            
        except Exception as e:
            logger.error(f"Error handling webhook: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            return web.Response(status=500, text="Internal server error")

    async def handle_transaction_webhook(self, payload: dict):
        """Handle transaction webhook"""
        try:
            addresses = payload.get('addresses', [])
            tx_hash = payload.get('tx_hash')
            
            for address in addresses:
                # Check if we're monitoring this address
                if address not in self.monitored_addresses:
                    continue
                    
                # Get user ID for this address
                user_id = await get_user_id_for_wallet(address)
                if not user_id:
                    continue
                    
                # Get transaction details
                tx = await self.rate_limited_request(
                    self.blockfrost_client.transaction,
                    hash=tx_hash
                )
                
                # Check if user wants transaction notifications
                if not await should_notify(user_id, 'ada_transactions'):
                    continue
                    
                # Create notification message
                message = (
                    f"🔔 **New Transaction**\n"
                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                    f"Transaction: `{tx_hash}`\n"
                    f"Block: `{payload['block']}`\n"
                    f"Confirmations: `{payload['confirmations']}`"
                )
                
                # Send notification
                await self.send_dm(int(user_id), message)
                
        except Exception as e:
            logger.error(f"Error handling transaction webhook: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

    async def handle_delegation_webhook(self, payload: dict):
        """Handle delegation webhook"""
        try:
            stake_address = payload.get('stake_address')
            pool_id = payload.get('pool_id')
            action = payload.get('action')
            
            if stake_address not in self.monitored_stake_addresses:
                return
                
            # Get all addresses for this stake address
            addresses = await get_addresses_for_stake(stake_address)
            if not addresses:
                return
                
            # Update pool for stake address
            if action == 'registered':
                await update_pool_for_stake(stake_address, pool_id)
            elif action == 'deregistered':
                await update_pool_for_stake(stake_address, None)
            
            for address in addresses:
                # Get user ID for this address
                user_id = await get_user_id_for_wallet(address)
                if not user_id:
                    continue
                    
                # Check if user wants delegation notifications
                if not await should_notify(user_id, 'delegation_status'):
                    continue
                    
                # Create notification message
                if action == 'registered':
                    message = (
                        f"🎯 **Delegation Update**\n"
                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                        f"Action: Delegated to pool\n"
                        f"Pool ID: `{pool_id[:8]}...{pool_id[-8:]}`"
                    )
                else:
                    message = (
                        f"🎯 **Delegation Update**\n"
                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                        f"Action: Stake key deregistered"
                    )
                
                # Send notification
                await self.send_dm(int(user_id), message)
                
        except Exception as e:
            logger.error(f"Error handling delegation webhook: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        
        # Create bot instance
        bot = WalletBud()
        
        # Get Discord token
        token = os.getenv('DISCORD_TOKEN')
        if not token:
            logger.error("DISCORD_TOKEN not found in environment variables")
            sys.exit(1)
            
        # Run the bot
        logger.info("Running bot...")
        bot.run(token, reconnect=True, log_handler=None)  # Let discord.py handle setup_hook
        
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        sys.exit(1)
