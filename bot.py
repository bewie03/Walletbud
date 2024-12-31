import os
import logging
import sys
from datetime import datetime
import time
import uuid
import asyncio
from functools import wraps
import random
import psutil

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi, ApiUrls

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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
    YUMMI_TOKEN_ID,
    MIN_YUMMI_REQUIREMENT,
    WALLET_CHECK_INTERVAL
)

from database import (
    get_pool,
    add_wallet,
    remove_wallet,
    get_user_id_for_wallet,
    get_all_wallets,
    get_notification_settings,
    update_notification_setting,
    initialize_notification_settings,
    get_wallet_for_user,
    init_db
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
        "staking_rewards": "staking_rewards",
        "stake_changes": "stake_changes",
        "low_balance": "low_balance",
        "policy_expiry": "policy_expiry",
        "delegation_status": "delegation_status",
        "dapp_interactions": "dapp_interactions",
        "failed_transactions": "failed_transactions",
        "asset_history": "asset_history"
    }
    
    NOTIFICATION_DISPLAY = {
        # Database key -> Display name
        "ada_transactions": "ADA Transactions",
        "token_changes": "Token Changes",
        "nft_updates": "NFT Updates",
        "staking_rewards": "Staking Rewards",
        "stake_changes": "Stake Key Changes",
        "low_balance": "Low Balance Alerts",
        "policy_expiry": "Policy Expiry Alerts",
        "delegation_status": "Delegation Status",
        "dapp_interactions": "DApp Interactions",
        "failed_transactions": "Failed Transactions",
        "asset_history": "Asset History"
    }
    
    DEFAULT_SETTINGS = {
        "ada_transactions": True,
        "token_changes": True,
        "nft_updates": True,
        "staking_rewards": True,
        "stake_changes": True,
        "low_balance": True,
        "policy_expiry": True,
        "delegation_status": True,
        "dapp_interactions": True,
        "failed_transactions": True,
        "asset_history": True
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
        super().__init__(
            command_prefix='!',
            intents=discord.Intents.all(),
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
        
    async def setup_hook(self):
        """Setup hook called before the bot starts"""
        try:
            logger.info("Starting bot initialization...")
            
            # Initialize database first
            logger.info("Initializing database...")
            await init_db()
            logger.info("Database initialized successfully")
            
            # Initialize Blockfrost client with retries
            logger.info("Initializing Blockfrost client...")
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost client - bot may have limited functionality")
            else:
                logger.info("Blockfrost client initialized successfully")
            
            # Set up commands
            logger.info("Setting up Discord commands...")
            try:
                await self.setup_commands()
                logger.info("Commands set up successfully")
                
                # Sync commands with Discord
                logger.info("Syncing commands with Discord...")
                await self.tree.sync()
                logger.info("Commands synced successfully")
            except Exception as e:
                logger.error(f"Failed to set up or sync commands: {str(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                raise
            
            # Start wallet monitoring task only if Blockfrost is initialized
            if self.blockfrost_client:
                logger.info("Starting wallet monitoring task...")
                self.check_wallets.start()
                logger.info("Wallet monitoring task started")
            else:
                logger.error("Skipping wallet monitoring task - Blockfrost client not initialized")
            
            # Start system monitoring
            self.bg_task = self.loop.create_task(self.monitor_system_health())
            
            # Start cleanup task
            self.cleanup_task.start()
            
            # Store start time for uptime tracking
            self.start_time = datetime.now()
            
            logger.info("Bot initialization completed")
            
        except Exception as e:
            logger.error(f"Critical error during bot initialization: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        MAX_RETRIES = 3
        RETRY_DELAY = 5  # seconds
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Creating Blockfrost client (attempt {attempt + 1}/{MAX_RETRIES})...")
                
                # Check project ID
                if not BLOCKFROST_PROJECT_ID:
                    logger.error("BLOCKFROST_PROJECT_ID not set")
                    return False
                    
                # Log first few characters of project ID for debugging
                logger.info(f"Using project ID starting with: {BLOCKFROST_PROJECT_ID[:8]}...")
                
                # Create client with correct project ID
                self.blockfrost_client = BlockFrostApi(
                    project_id=BLOCKFROST_PROJECT_ID
                )
                logger.info("BlockFrostApi client created successfully")
                
                # Test connection with address endpoint
                try:
                    logger.info("Testing Blockfrost connection...")
                    # Use a known valid address to test connection
                    test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
                    logger.info(f"Testing with address: {test_address[:20]}...")
                    
                    # Test basic address info
                    logger.info("Testing address info endpoint...")
                    async with self.rate_limiter:
                        address_info = await asyncio.to_thread(self.blockfrost_client.address, test_address)
                        if not address_info:
                            raise Exception("Failed to get address info")
                        logger.info("Basic address info test passed")
                    
                    # Get extended address info
                    async with self.rate_limiter:
                        extended_info = await asyncio.to_thread(self.blockfrost_client.address_extended, test_address)
                        if not extended_info:
                            raise Exception("Failed to get extended address info")
                        logger.info("Extended address info test passed")
                    
                    # Get address total
                    async with self.rate_limiter:
                        total_info = await asyncio.to_thread(self.blockfrost_client.address_total, test_address)
                        if not total_info:
                            raise Exception("Failed to get address total")
                        logger.info("Address total test passed")
                    
                    # Test UTXOs
                    logger.info("Testing UTXOs endpoint...")
                    async with self.rate_limiter:
                        utxos = await asyncio.to_thread(self.blockfrost_client.address_utxos, test_address)
                        if not utxos:
                            raise Exception("Failed to get UTXOs")
                        logger.info("UTXOs test passed")
                    
                    logger.info("All Blockfrost connection tests passed")
                    return True
                    
                except Exception as e:
                    logger.error(f"Blockfrost test failed: {str(e)}")
                    raise
                        
            except Exception as e:
                logger.error(f"Failed to create Blockfrost client: {str(e)}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Response details: {e.response.text}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                self.blockfrost_client = None
                
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    await asyncio.sleep(RETRY_DELAY)
                continue
        
        logger.error(f"Failed to initialize Blockfrost client after {MAX_RETRIES} attempts")
        return False

    async def rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited request to the Blockfrost API with retry logic"""
        max_retries = 3
        base_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                async with self.rate_limiter:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                return result
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
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

    async def check_yummi_requirement(self, address: str):
        """Check if wallet meets YUMMI token requirement"""
        try:
            # Get all assets for the address
            assets = await self.rate_limited_request(self.blockfrost_client.address_assets, address)
            
            # Find YUMMI token
            yummi_balance = 0
            for asset in assets:
                if asset.unit == YUMMI_TOKEN_ID:
                    yummi_balance = int(asset.quantity)
                    break
            
            meets_requirement = yummi_balance >= MIN_YUMMI_REQUIREMENT
            return meets_requirement, yummi_balance
            
        except Exception as e:
            logger.error(f"Error checking YUMMI requirement for {address}: {str(e)}")
            return False, 0

    @tasks.loop(seconds=WALLET_CHECK_INTERVAL)
    async def check_wallets(self):
        """Background task to check all wallets periodically"""
        if self.monitoring_paused or self.processing_wallets:
            logger.info("Skipping wallet check - monitoring paused or already processing")
            return
            
        try:
            async with self.wallet_task_lock:
                self.processing_wallets = True
                logger.info("Starting wallet check cycle...")
                
                # Verify Blockfrost client
                if not self.blockfrost_client:
                    logger.error("Cannot check wallets - Blockfrost client not initialized")
                    # Try to reinitialize
                    if not await self.init_blockfrost():
                        logger.error("Failed to reinitialize Blockfrost client")
                        return
                
                # Get all wallets
                wallets = await get_all_wallets()
                if not wallets:
                    logger.info("No wallets to check")
                    return
                    
                logger.info(f"Checking {len(wallets)} wallets...")
                
                for wallet in wallets:
                    try:
                        await self.check_wallet(wallet['address'])
                    except Exception as e:
                        logger.error(f"Error checking wallet {wallet['address']}: {str(e)}")
                        if hasattr(e, '__dict__'):
                            logger.error(f"Error details: {e.__dict__}")
                        continue
                
                logger.info("Wallet check cycle completed")
                
        except Exception as e:
            logger.error(f"Critical error in wallet check task: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            await self.send_admin_alert(f"Critical error in wallet monitoring: {str(e)}")
            
        finally:
            self.processing_wallets = False

    async def check_wallet(self, address: str):
        """Check a single wallet's balance and transactions"""
        try:
            async with self.wallet_task_lock:
                # Get user ID for the wallet
                user_id = await get_user_id_for_wallet(address)
                if not user_id:
                    logger.error(f"No user found for wallet {address}")
                    return
                    
                # Get wallet details
                wallet = await get_wallet_for_user(user_id, address)
                if not wallet:
                    logger.error(f"No wallet found for user {user_id} and address {address}")
                    return
                    
                wallet_id = wallet['id']

                # Check YUMMI requirement every 6 hours
                last_check = await get_last_yummi_check(address)
                if not last_check or (datetime.now() - last_check).total_seconds() > 21600:  # 6 hours
                    meets_req, balance = await self.check_yummi_requirement(address)
                    await update_last_yummi_check(address)
                    
                    if not meets_req:
                        # Increment warning count and get new count
                        warning_count = await increment_yummi_warning(wallet_id)
                        
                        # Remove wallet after 3 warnings
                        if warning_count >= 3:
                            logger.warning(f"Removing wallet {address} due to insufficient YUMMI balance")
                            await remove_wallet(wallet_id)
                            
                            if await should_notify(user_id, "low_balance"):
                                await self.send_dm(
                                    user_id,
                                    f"❌ Your wallet `{address[:8]}...{address[-8:]}` has been removed from monitoring due to "
                                    f"insufficient YUMMI token balance ({balance} < {MIN_YUMMI_REQUIREMENT})"
                                )
                            return
                        
                        # Send warning
                        if await should_notify(user_id, "low_balance"):
                            await self.send_dm(
                                user_id,
                                f"⚠️ Warning ({warning_count}/3): Your wallet `{address[:8]}...{address[-8:]}` has insufficient "
                                f"YUMMI token balance ({balance} < {MIN_YUMMI_REQUIREMENT}). Please add more YUMMI tokens to "
                                "continue monitoring."
                            )
                
                # Get current wallet state
                try:
                    # Get current transactions
                    current_txs = await self.rate_limited_request(
                        self.blockfrost_client.address_transactions,
                        address=address,
                        count=50  # Limit to recent transactions
                    )
                    
                    # Get current UTXOs
                    current_utxos = await self.rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address=address
                    )
                    
                    # Get extended info for delegation status
                    extended_info = await self.rate_limited_request(
                        self.blockfrost_client.address_extended,
                        address=address
                    )
                    
                    # Process each check in parallel
                    await asyncio.gather(
                        self._check_policy_expiry(address, user_id),
                        self._check_delegation_status(address, user_id, extended_info),
                        self._check_dapp_interactions(address, user_id, current_txs)
                    )
                    
                except Exception as e:
                    logger.error(f"Error getting wallet state for {address}: {str(e)}")
                    if hasattr(e, '__dict__'):
                        logger.error(f"Error details: {e.__dict__}")
                    return
                
                # Get and update stake address
                stake_address = await self.rate_limited_request(
                    self.blockfrost_client.address_stake,
                    address
                )
                if stake_address:
                    await update_stake_address(address, stake_address)

                # Get current UTxO state
                current_utxos = await self.rate_limited_request(
                    self.blockfrost_client.address_utxos,
                    address
                )
                
                if not current_utxos:
                    logger.error(f"Failed to get UTXOs for {address}")
                    return

                # Get previous UTxO state to check for changes
                previous_state = await get_utxo_state(address)
                
                # Update UTxO state
                new_state = {
                    'utxos': [utxo.tx_hash for utxo in current_utxos],
                    'last_updated': datetime.now().isoformat()
                }
                await update_utxo_state(address, new_state)
                
                # Check for UTxO changes
                if previous_state:
                    prev_utxos = set(previous_state.get('utxos', []))
                    current_utxos_set = set(new_state['utxos'])
                    
                    # Notify about significant UTxO changes
                    if prev_utxos != current_utxos_set and await should_notify(user_id, "utxo_changes"):
                        spent_utxos = prev_utxos - current_utxos_set
                        new_utxos = current_utxos_set - prev_utxos
                        
                        if spent_utxos or new_utxos:
                            message = f"📝 UTxO Changes Detected for `{address[:8]}...{address[-8:]}`:\n"
                            if new_utxos:
                                message += f"New UTxOs: {len(new_utxos)}\n"
                            if spent_utxos:
                                message += f"Spent UTxOs: {len(spent_utxos)}\n"
                            await self.send_dm(int(user_id), message)
                
                # Calculate ADA balance and token balances
                ada_balance = 0
                token_balances = {}
                
                for utxo in current_utxos:
                    if utxo.amount:
                        for amount in utxo.amount:
                            if amount.unit == 'lovelace':
                                ada_balance += int(amount.quantity)
                            else:
                                token_id = amount.unit
                                current_amount = token_balances.get(token_id, 0)
                                token_balances[token_id] = current_amount + int(amount.quantity)
                
                ada_balance = ada_balance / 1_000_000  # Convert to ADA
                
                # Update token balances in database
                await update_token_balances(address, token_balances)
                
                # Get previous balance for comparison
                previous_balance = await get_wallet_balance(address)
                
                # Check for low ADA balance
                if ada_balance < 5 and await should_notify(user_id, "low_balance"):
                    await self.send_dm(
                        int(user_id),
                        f"⚠️ Low ADA balance warning!\n"
                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                        f"Current balance: {ada_balance:.2f} ₳"
                    )
                
                # Check for significant balance changes
                if previous_balance and abs(ada_balance - previous_balance) > 1:  # 1 ADA threshold
                    if await should_notify(user_id, "ada_transactions"):
                        change = ada_balance - previous_balance
                        await self.send_dm(
                            int(user_id),
                            f"💰 Balance Change Detected!\n"
                            f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                            f"{'Received' if change > 0 else 'Sent'}: {abs(change):.2f} ₳\n"
                            f"New Balance: {ada_balance:.2f} ₳"
                        )
                
                # Update wallet balance
                await update_ada_balance(address, ada_balance)
                
                # Get and process recent transactions
                recent_txs = await self.rate_limited_request(
                    self.blockfrost_client.address_transactions,
                    address=address,
                    count=50  # Limit to recent transactions
                )
                if recent_txs:
                    last_known_txs = await get_last_transactions(address) or []
                    
                    for tx in recent_txs:
                        if tx.hash not in last_known_txs:
                            # Add new transaction to database
                            await add_transaction(wallet_id, tx.hash)
                            
                            # Check for staking rewards
                            if not await is_reward_processed(tx.hash):
                                rewards = await self.rate_limited_request(
                                    self.blockfrost_client.transaction_stakes,
                                    tx.hash
                                )
                                
                                if rewards:
                                    for reward in rewards:
                                        if reward.cert_index == 0:  # Reward certificate
                                            await add_processed_reward(tx.hash)
                                            if await should_notify(user_id, "staking_rewards"):
                                                await self.send_dm(
                                                    int(user_id),
                                                    f"🎉 Staking Reward Received!\n"
                                                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                                    f"Amount: {float(reward.amount) / 1_000_000:.2f} ₳\n"
                                                    f"Transaction: `{tx.hash}`"
                                                )
                            
                            # Process transaction metadata for DApp interactions
                            if await should_notify(user_id, "dapp_interactions"):
                                metadata = await get_transaction_metadata(wallet_id, tx.hash)
                                if metadata:
                                    for field in self.metadata_fields:
                                        if field in metadata:
                                            value = str(metadata[field]).lower()
                                            for dapp, patterns in self.dapp_patterns.items():
                                                if any(pattern in value for pattern in patterns):
                                                    await self.send_dm(
                                                        int(user_id),
                                                        f"🔄 DApp Interaction Detected!\n"
                                                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                                        f"DApp: {dapp.title()}\n"
                                                        f"Transaction: `{tx.hash}`"
                                                    )
                                                    break
                
                # Update last checked timestamp
                await update_last_checked(wallet_id)
                
        except Exception as e:
            logger.error(f"Error checking wallet {address}: {str(e)}")

    async def _check_policy_expiry(self, address: str, user_id: int):
        """Check for expiring token policies"""
        try:
            # Get all tokens in wallet
            assets = await self.rate_limited_request(
                self.blockfrost_client.address_assets,
                address
            )
            
            for asset in assets:
                policy_id = asset.unit[:56]  # First 56 chars are policy ID
                
                # Get policy expiry info from Blockfrost
                policy = await self.rate_limited_request(
                    self.blockfrost_client.pool,
                    policy_id
                )
                
                if policy and policy.expiry:
                    # Get stored expiry time
                    stored_expiry = await get_policy_expiry(address, policy_id)
                    
                    if not stored_expiry:
                        # New policy, store expiry
                        await update_policy_expiry(address, policy_id, policy.expiry)
                        continue
                        
                    # Check if expiring soon (within 30 days)
                    expiry_date = datetime.fromtimestamp(policy.expiry)
                    days_until_expiry = (expiry_date - datetime.now()).days
                    
                    if days_until_expiry <= 30 and await self.should_notify(int(user_id), "policy_expiry"):
                        await self.send_dm(
                            int(user_id),
                            f"⚠️ **Token Policy Expiring Soon!**\n"
                            f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                            f"Policy ID: `{policy_id}`\n"
                            f"Days Until Expiry: `{days_until_expiry}`"
                        )
                        
        except Exception as e:
            logger.error(f"Error checking policy expiry: {str(e)}")

    async def _check_delegation_status(self, address: str, user_id: int, extended_info):
        """Check for delegation status changes"""
        try:
            # Get delegation info
            delegation = await self.rate_limited_request(
                self.blockfrost_client.account_delegations,
                extended_info.stake_address,
                count=1,
                page=1
            )
            
            if delegation:
                current_pool = delegation[0].pool_id
                stored_pool = await get_delegation_status(address)
                
                if current_pool != stored_pool:
                    await update_delegation_status(address, current_pool)
                    
                    if await self.should_notify(int(user_id), "delegation_status"):
                        pool_info = await self.rate_limited_request(
                            self.blockfrost_client.pool,
                            current_pool
                        )
                        pool_name = pool_info.metadata.name if pool_info.metadata else "Unknown Pool"
                        
                        await self.send_dm(
                            int(user_id),
                            f"🔄 **Delegation Status Changed**\n"
                            f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                            f"New Pool: `{pool_name}`\n"
                            f"Pool ID: `{current_pool}`"
                        )
                            
        except Exception as e:
            logger.error(f"Error checking delegation status: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response details: {e.response.text}")

    async def _check_dapp_interactions(self, address: str, user_id: int, current_txs):
        """Check for DApp interactions"""
        try:
            # Get last processed tx
            last_processed = await get_dapp_interactions(address)
            
            for tx in current_txs:
                if last_processed and tx.hash == last_processed:
                    break
                    
                # Check transaction metadata
                metadata = await self.rate_limited_request(
                    self.blockfrost_client.transaction_metadata,
                    tx.hash
                )
                
                if not metadata:
                    continue
                    
                # Look for DApp identifiers in metadata
                dapp_found = False
                dapp_name = None
                
                for field in self.metadata_fields:
                    for meta in metadata:
                        if field.lower() in str(meta.label).lower():
                            # Check against known DApp patterns
                            meta_str = str(meta.json_metadata).lower()
                            for name, patterns in self.dapp_patterns.items():
                                if any(pattern in meta_str for pattern in patterns):
                                    dapp_found = True
                                    dapp_name = name
                                    break
                            if dapp_found:
                                break
                        
                if dapp_found and await self.should_notify(int(user_id), "dapp_interactions"):
                    await self.send_dm(
                        int(user_id),
                        f"🔄 **New DApp Interaction Detected!**\n"
                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                        f"DApp: `{dapp_name}`\n"
                        f"Transaction: `{tx.hash}`"
                    )
                    
                # Update last processed tx
                await update_dapp_interaction(address, tx.hash)
                
        except Exception as e:
            logger.error(f"Error checking DApp interactions: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response details: {e.response.text}")

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

    async def setup_commands(self):
        """Set up bot commands"""
        try:
            logger.info("Setting up commands...")
            
            # Wallet management commands
            @self.tree.command(name="addwallet", description="Register a wallet to monitor")
            @dm_only()
            @app_commands.describe(address="The wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                await self._add_wallet(interaction, address)

            @self.tree.command(name="removewallet", description="Stop monitoring a wallet")
            @dm_only()
            @app_commands.describe(address="The wallet address to remove")
            async def removewallet(interaction: discord.Interaction, address: str):
                await self._remove_wallet(interaction, address)

            @self.tree.command(name="listwallets", description="List your registered wallets")
            @dm_only()
            async def listwallets(interaction: discord.Interaction):
                await self._list_wallets(interaction)

            @self.tree.command(name="help", description="Show bot help and commands")
            async def help(interaction: discord.Interaction):
                await self._help(interaction)

            @self.tree.command(name="health", description="Check bot and API status")
            async def health(interaction: discord.Interaction):
                await self._health(interaction)

            # Balance and notification commands
            @self.tree.command(name="balance", description="Get your wallet's current balance")
            @dm_only()
            async def balance(interaction: discord.Interaction):
                await self._balance(interaction)

            @self.tree.command(name="notifications", description="Manage your notification settings")
            @dm_only()
            @app_commands.choices(action=[
                app_commands.Choice(name="view", value="view"),
                app_commands.Choice(name="enable", value="enable"),
                app_commands.Choice(name="disable", value="disable")
            ])
            @app_commands.choices(notification_type=[
                app_commands.Choice(name="ada_transactions", value="ada_transactions"),
                app_commands.Choice(name="token_changes", value="token_changes"),
                app_commands.Choice(name="nft_updates", value="nft_updates"),
                app_commands.Choice(name="staking_rewards", value="staking_rewards"),
                app_commands.Choice(name="stake_changes", value="stake_changes"),
                app_commands.Choice(name="low_balance", value="low_balance"),
                app_commands.Choice(name="policy_expiry", value="policy_expiry"),
                app_commands.Choice(name="delegation_status", value="delegation_status"),
                app_commands.Choice(name="dapp_interactions", value="dapp_interactions"),
                app_commands.Choice(name="failed_transactions", value="failed_transactions"),
                app_commands.Choice(name="asset_history", value="asset_history")
            ])
            async def notifications(interaction: discord.Interaction, action: str, notification_type: str = None):
                """Manage notification settings"""
                try:
                    user_id = str(interaction.user.id)
                    
                    if action == "view":
                        settings = await get_notification_settings(user_id)
                        
                        embed = discord.Embed(
                            title="🔔 Your Notification Settings",
                            description="Here are your current notification settings:",
                            color=discord.Color.blue()
                        )
                        
                        for ntype, enabled in settings.items():
                            status = "✅ Enabled" if enabled else "❌ Disabled"
                            embed.add_field(
                                name=ntype.replace("_", " ").title(),
                                value=status,
                                inline=True
                            )
                        
                        await interaction.response.send_message(embed=embed)
                        
                    elif notification_type:
                        await update_notification_setting(user_id, notification_type, action == "enable")
                        status = "enabled" if action == "enable" else "disabled"
                        
                        embed = discord.Embed(
                            title="✅ Notification Setting Updated",
                            description=f"{notification_type.replace('_', ' ').title()} notifications have been {status}.",
                            color=discord.Color.green()
                        )
                        
                        await interaction.response.send_message(embed=embed)
                    else:
                        await interaction.response.send_message(
                            "Please specify a notification type to enable/disable.",
                            ephemeral=True
                        )
                        
                except Exception as e:
                    logger.error(f"Error in notifications command: {str(e)}")
                    await interaction.response.send_message(
                        "❌ An error occurred while managing notifications.",
                        ephemeral=True
                    )

            @self.tree.command(name="check", description="Check various wallet information")
            @app_commands.choices(check_type=[
                app_commands.Choice(name="balance", value="balance"),
                app_commands.Choice(name="transactions", value="transactions"),
                app_commands.Choice(name="nfts", value="nfts"),
                app_commands.Choice(name="stake", value="stake"),
                app_commands.Choice(name="failed", value="failed"),
                app_commands.Choice(name="history", value="history")
            ])
            async def check(interaction: discord.Interaction, check_type: str):
                """Check various wallet information"""
                try:
                    user_id = str(interaction.user.id)
                    wallets = await get_user_wallets(user_id)
                    
                    if not wallets:
                        await interaction.response.send_message(
                            "❌ No wallets found. Please add a wallet first using `/addwallet`.",
                            ephemeral=True
                        )
                        return
                        
                    await interaction.response.defer()
                    
                    if check_type == "balance":
                        embed = discord.Embed(
                            title="💰 Wallet Balance",
                            color=discord.Color.blue()
                        )
                        
                        for wallet in wallets:
                            balance = await get_wallet_balance(wallet)
                            if balance:
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=f"ADA: {balance['ada']} ₳\nTokens: {len(balance['tokens'])}",
                                    inline=False
                                )
                                
                                # Add token details if any
                                if balance['tokens']:
                                    token_text = ""
                                    for token, amount in balance['tokens'].items():
                                        token_text += f"{token}: {amount}\n"
                                    embed.add_field(
                                        name="Token Details",
                                        value=token_text[:1024],  # Discord field value limit
                                        inline=False
                                    )
                        
                        await interaction.followup.send(embed=embed)
                        
                    elif check_type == "transactions":
                        embed = discord.Embed(
                            title="📝 Recent Transactions",
                            color=discord.Color.blue()
                        )
                        
                        for wallet in wallets:
                            txs = await get_recent_transactions(wallet)
                            if txs:
                                tx_text = ""
                                for tx in txs[:5]:  # Show last 5 transactions
                                    tx_text += f"Hash: [{tx['hash'][:8]}...](https://cardanoscan.io/transaction/{tx['hash']})\n"
                                    tx_text += f"Amount: {tx['amount']} ₳\n"
                                    tx_text += f"Date: {tx['date']}\n\n"
                                
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=tx_text or "No recent transactions",
                                    inline=False
                                )
                        
                        await interaction.followup.send(embed=embed)
                        
                    elif check_type == "nfts":
                        embed = discord.Embed(
                            title="🎨 NFT Collection",
                            color=discord.Color.blue()
                        )
                        
                        for wallet in wallets:
                            nfts = await get_nfts(wallet)
                            if nfts:
                                nft_text = ""
                                for nft in nfts[:5]:  # Show first 5 NFTs
                                    nft_text += f"Name: {nft['name']}\n"
                                    nft_text += f"Policy: {nft['policy_id'][:8]}...\n\n"
                                
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=nft_text or "No NFTs found",
                                    inline=False
                                )
                                
                                # Add thumbnail of first NFT if it has an image
                                if nfts and nfts[0].get('image'):
                                    embed.set_thumbnail(url=nfts[0]['image'])
                        
                        await interaction.followup.send(embed=embed)
                        
                    elif check_type == "stake":
                        embed = discord.Embed(
                            title="🎯 Staking Information",
                            color=discord.Color.blue()
                        )
                        
                        for wallet in wallets:
                            stake_info = await get_stake_info(wallet)
                            if stake_info:
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=f"Pool: {stake_info['pool_id']}\n"
                                          f"Rewards: {stake_info['rewards']} ₳\n"
                                          f"Delegated: {stake_info['delegated']} ₳",
                                    inline=False
                                )
                        
                        await interaction.followup.send(embed=embed)
                        
                    elif check_type == "failed":
                        embed = discord.Embed(
                            title="❌ Failed Transactions",
                            color=discord.Color.red()
                        )
                        
                        for wallet in wallets:
                            failed_txs = await get_failed_transactions(wallet)
                            if failed_txs:
                                tx_text = ""
                                for tx in failed_txs[:5]:  # Show last 5 failed transactions
                                    tx_text += f"Hash: [{tx['hash'][:8]}...](https://cardanoscan.io/transaction/{tx['hash']})\n"
                                    tx_text += f"Type: {tx['type']}\n"
                                    tx_text += f"Error: {tx['error']}\n\n"
                                
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=tx_text or "No failed transactions",
                                    inline=False
                                )
                        
                        await interaction.followup.send(embed=embed)
                        
                    elif check_type == "history":
                        embed = discord.Embed(
                            title="📊 Asset History",
                            color=discord.Color.blue()
                        )
                        
                        for wallet in wallets:
                            history = await get_asset_history(wallet)
                            if history:
                                history_text = ""
                                for entry in history[:5]:  # Show last 5 history entries
                                    history_text += f"Asset: {entry['asset_id'][:8]}...\n"
                                    history_text += f"Action: {entry['action']}\n"
                                    history_text += f"Amount: {entry['quantity']}\n"
                                    history_text += f"Date: {entry['date']}\n\n"
                                
                                embed.add_field(
                                    name=f"Wallet {wallet[:8]}...",
                                    value=history_text or "No asset history",
                                    inline=False
                                )
                        
                        await interaction.followup.send(embed=embed)
                        
                except Exception as e:
                    logger.error(f"Error in check command: {str(e)}")
                    await interaction.followup.send(
                        "❌ An error occurred while checking wallet information.",
                        ephemeral=True
                    )
            
            logger.info("Commands set up successfully")
            
        except Exception as e:
            logger.error(f"Error setting up commands: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise

    @commands.Cog.listener()
    async def on_ready(self):
        """Called when bot is ready"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            logger.info(f"Discord API version: {discord.__version__}")
            logger.info(f"Connected to {len(self.guilds)} guilds")
            
            # Log guild information
            for guild in self.guilds:
                logger.info(f"Connected to guild: {guild.name} ({guild.id})")
                
            # Initialize Blockfrost
            await self.init_blockfrost()
            
            # Start monitoring task
            self.bg_task = self.loop.create_task(self.check_wallets())
            logger.info("Started wallet monitoring task")
            
            logger.info("Bot initialization complete!")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Called when bot joins a guild"""
        logger.info(f"Joined guild {guild.name} ({guild.id})")

    async def monitor_system_health(self):
        """Background task to monitor system health"""
        while True:
            try:
                # Get system metrics
                process = psutil.Process()
                memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                cpu_percent = process.cpu_percent()
                
                # Check database connection
                db_healthy = await self.check_database()
                
                # Check Blockfrost API
                bf_healthy = await self.check_blockfrost()
                
                # Check monitored wallets
                total_wallets = len(await get_all_wallets())
                
                # Define thresholds
                MEMORY_THRESHOLD = 500  # MB
                CPU_THRESHOLD = 80  # percent
                
                # Prepare status message
                status = []
                
                if memory_usage > MEMORY_THRESHOLD:
                    status.append(f"⚠️ High memory usage: {memory_usage:.1f} MB")
                
                if cpu_percent > CPU_THRESHOLD:
                    status.append(f"⚠️ High CPU usage: {cpu_percent}%")
                
                if not db_healthy:
                    status.append("❌ Database connection issues")
                
                if not bf_healthy:
                    status.append("❌ Blockfrost API issues")
                
                # Send alert if any issues
                if status:
                    await self.send_admin_alert(
                        "System Health Alert:\n" + "\n".join(status),
                        is_error=True
                    )
                
                # Log metrics
                logger.info(
                    f"Health Check - Memory: {memory_usage:.1f}MB, CPU: {cpu_percent}%, "
                    f"DB: {'✅' if db_healthy else '❌'}, "
                    f"API: {'✅' if bf_healthy else '❌'}, "
                    f"Wallets: {total_wallets}"
                )
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying

    @tasks.loop(minutes=5)
    async def cleanup_task(self):
        """Cleanup task to maintain system health"""
        try:
            # 1. Clean up old notifications
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """DELETE FROM notifications 
                    WHERE created_at < NOW() - INTERVAL '30 days'"""
                )
            
            # 2. Remove inactive wallets
            inactive_days = 90
            await conn.execute(
                """DELETE FROM wallets 
                WHERE last_activity < NOW() - INTERVAL '%s days'""",
                inactive_days
            )
            
            # 3. Optimize database
            await conn.execute("VACUUM ANALYZE")
            
            logger.info("Cleanup task completed successfully")
            
        except Exception as e:
            logger.error(f"Error in cleanup task: {str(e)}")
            await self.send_admin_alert(
                f"❌ Cleanup task failed: {str(e)}",
                is_error=True
            )

    async def check_database(self) -> bool:
        """Check database connection and health"""
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                # Check connection
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    return False
                    
                # Check table health
                tables = ['wallets', 'notifications', 'transactions', 'rewards']
                for table in tables:
                    await conn.execute(f"SELECT COUNT(*) FROM {table}")
                    
                return True
                
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            return False

    async def check_blockfrost(self) -> bool:
        """Check Blockfrost API health and rate limits"""
        try:
            if not self.blockfrost_client:
                return False
                
            # Test API connection
            health = await self.rate_limited_request(
                self.blockfrost_client.health
            )
            
            if not health or health.is_healthy is False:
                return False
                
            # Check rate limits
            limits = await self.rate_limited_request(
                self.blockfrost_client.health_clock
            )
            
            if not limits:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Blockfrost health check failed: {str(e)}")
            return False

    @self.tree.command(name="health", description="Check bot health status")
    async def health(interaction: discord.Interaction):
        """Enhanced health check command"""
        try:
            # System metrics
            process = psutil.Process()
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            cpu_percent = process.cpu_percent()
            
            # Database check
            db_status = await self.check_database()
            
            # Blockfrost check
            bf_status = await self.check_blockfrost()
            
            # Get bot uptime
            uptime = datetime.now() - self.start_time
            hours = uptime.total_seconds() / 3600
            
            # Get wallet stats
            total_wallets = len(await get_all_wallets())
            
            # Create rich embed
            embed = discord.Embed(
                title="🏥 Bot Health Status",
                color=discord.Color.green() if db_status and bf_status else discord.Color.red(),
                timestamp=datetime.now()
            )
            
            # Core Services
            embed.add_field(
                name="Core Services",
                value=f"""
                • Database: {'✅' if db_status else '❌'}
                • Blockfrost API: {'✅' if bf_status else '❌'}
                """.strip(),
                inline=False
            )
            
            # System Metrics
            embed.add_field(
                name="System Metrics",
                value=f"""
                • Memory Usage: {memory_usage:.1f} MB
                • CPU Usage: {cpu_percent}%
                • Uptime: {hours:.1f} hours
                """.strip(),
                inline=False
            )
            
            # Bot Stats
            embed.add_field(
                name="Bot Statistics",
                value=f"""
                • Monitored Wallets: {total_wallets}
                • Commands Available: {len(self.tree.get_commands())}
                """.strip(),
                inline=False
            )
            
            # Rate Limits
            if bf_status:
                limits = await self.rate_limited_request(
                    self.blockfrost_client.health_clock
                )
                embed.add_field(
                    name="API Rate Limits",
                    value=f"""
                    • Remaining Calls: {limits.server_time}
                    • Reset in: {limits.server_time_diff} seconds
                    """.strip(),
                    inline=False
                )
            
            # Add version info
            embed.set_footer(text=f"Bot Version: {BOT_VERSION}")
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            logger.error(f"Error checking health: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred while checking bot health.",
                ephemeral=True
            )

    async def _add_wallet(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            # Check if wallet is already being monitored
            existing = await get_wallet_for_user(str(interaction.user.id), address)
            if existing:
                await interaction.response.send_message(
                    f"❌ Wallet `{address[:8]}...{address[-8:]}` is already being monitored.",
                    ephemeral=True
                )
                return
            
            # Add wallet to database
            success = await add_wallet(str(interaction.user.id), address)
            if not success:
                await interaction.response.send_message(
                    "❌ Failed to add wallet. Please try again later.",
                    ephemeral=True
                )
                return
            
            # Initialize notification settings
            await initialize_notification_settings(str(interaction.user.id))
            
            await interaction.response.send_message(
                f"✅ Added wallet `{address[:8]}...{address[-8:]}` to monitoring.",
                ephemeral=True
            )
            
            # Check wallet immediately
            await self.check_wallet(address)
            
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred while adding the wallet. Please try again later.",
                ephemeral=True
            )

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists
            wallet = await get_wallet_for_user(str(interaction.user.id), address)
            if not wallet:
                await interaction.response.send_message("❌ Wallet not found!", ephemeral=True)
                return
            
            # Reset YUMMI warnings
            await reset_yummi_warning(wallet['id'])
            
            # Remove wallet
            success = await remove_wallet(str(interaction.user.id), address)
            if success:
                await interaction.response.send_message("✅ Wallet removed successfully!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to remove wallet.", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while removing the wallet", ephemeral=True)

    async def _list_wallets(self, interaction: discord.Interaction):
        """List all registered wallets"""
        try:
            # Get user's wallets
            addresses = await get_all_wallets()
            if not addresses:
                await self.send_response(
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
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while listing your wallets", ephemeral=True)

    async def _help(self, interaction: discord.Interaction):
        """Show bot help and commands"""
        try:
            embed = discord.Embed(
                title="🤖 WalletBud Help",
                description=(
                    "Monitor your Cardano wallets and receive notifications about important events!\n\n"
                    "**Note:** You must maintain the required YUMMI token balance to use this bot. "
                    "After 3 warnings for insufficient YUMMI balance, your wallet will be automatically removed."
                ),
                color=discord.Color.blue()
            )
            
            # Wallet Management
            embed.add_field(
                name="📝 Wallet Management",
                value=(
                    "`/addwallet <address>` - Register a wallet to monitor\n"
                    "`/removewallet <address>` - Stop monitoring a wallet\n"
                    "`/listwallets` - List your registered wallets\n"
                    "`/balance` - View current balance of all wallets"
                ),
                inline=False
            )
            
            # Notifications
            embed.add_field(
                name="🔔 Notifications",
                value=(
                    "`/notifications` - Manage your notification settings\n"
                    "`/toggle <type>` - Toggle a notification type on/off\n\n"
                    "**Notification Types:**\n"
                    "• `balance` - Low balance alerts\n"
                    "• `delegation` - Delegation status changes\n"
                    "• `dapp` - DApp interactions\n"
                    "• `nft` - NFT updates\n"
                    "• `policy` - Policy expiry alerts\n"
                    "• `stake` - Stake key changes\n"
                    "• `staking` - Staking rewards\n"
                    "• `token` - Token changes"
                ),
                inline=False
            )
            
            # System
            embed.add_field(
                name="🔧 System",
                value=(
                    "`/help` - Show this help message\n"
                    "`/health` - Check bot and API status"
                ),
                inline=False
            )
            
            # Footer
            embed.set_footer(text="WalletBud is monitoring the Cardano blockchain 24/7!")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing help: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while showing help.", ephemeral=True)

    async def _health(self, interaction: discord.Interaction):
        """Check bot and API status"""
        try:
            # Create embed
            embed = discord.Embed(
                title="🏥 System Health",
                description="Current status of WalletBud systems:",
                color=discord.Color.green()
            )
            
            # Check Discord connection
            discord_status = "✅ Connected" if self.is_ready() else "❌ Disconnected"
            embed.add_field(
                name="Discord Bot",
                value=f"{discord_status}\nLatency: `{round(self.latency * 1000)}ms`",
                inline=False
            )
            
            # Check Blockfrost API
            try:
                # Test with a known address
                test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
                async with self.rate_limiter:
                    await asyncio.to_thread(self.blockfrost_client.address, test_address)
                blockfrost_status = "✅ Connected"
            except Exception as e:
                logger.error(f"Blockfrost health check failed: {str(e)}")
                blockfrost_status = "❌ Error"
                
            embed.add_field(
                name="Blockfrost API",
                value=blockfrost_status,
                inline=False
            )
            
            # Add monitoring status
            monitoring_status = "✅ Active" if not self.monitoring_paused else "⏸️ Paused"
            embed.add_field(
                name="Wallet Monitoring",
                value=monitoring_status,
                inline=False
            )
            
            # Add uptime if available
            if hasattr(self, 'start_time'):
                uptime = datetime.now() - self.start_time
                days = uptime.days
                hours, remainder = divmod(uptime.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
                embed.add_field(
                    name="Uptime",
                    value=f"`{uptime_str}`",
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error checking health: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while checking system health.", ephemeral=True)

    async def _balance(self, interaction: discord.Interaction):
        """Get your wallet's current balance"""
        try:
            # Get user's wallets
            wallets = await get_all_wallets()
            if not wallets:
                await self.send_response(
                    interaction,
                    "❌ You don't have any wallets registered! Use `/addwallet` to add one.",
                    ephemeral=True
                )
                return
            
            # Create embed
            embed = discord.Embed(
                title="💰 Wallet Balances",
                description="Here are the current balances for all your registered wallets:",
                color=discord.Color.green()
            )
            
            # Add fields for each wallet
            for i, address in enumerate(wallets, 1):
                try:
                    # Get wallet details
                    wallet = await get_wallet_for_user(str(interaction.user.id), address)
                    if not wallet:
                        logger.error(f"No wallet found for address {address}")
                        continue
                        
                    wallet_id = wallet['id']
                    
                    # Get UTXOs for balance calculation
                    utxos = await self.rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    if utxos:
                        # Calculate ADA balance
                        ada_balance = sum(
                            int(utxo.amount[0].quantity) 
                            for utxo in utxos 
                            if utxo.amount and utxo.amount[0].unit == 'lovelace'
                        ) / 1_000_000
                        
                        # Calculate YUMMI balance
                        yummi_balance = sum(
                            int(amount.quantity)
                            for utxo in utxos
                            for amount in utxo.amount
                            if amount.unit.lower() == YUMMI_TOKEN_ID.lower()
                        )
                        
                        # Get transaction count
                        tx_info = await self.rate_limited_request(
                            self.blockfrost_client.address_total,
                            address
                        )
                        tx_count = tx_info.tx_count if tx_info else 0
                        
                        # Get warning count
                        warning_count = await get_yummi_warning_count(wallet_id) if wallet_id else 0
                        
                        # Format address for display
                        short_address = f"{address[:8]}...{address[-8:]}"
                        
                        # Add wallet field
                        embed.add_field(
                            name=f"Wallet {i}",
                            value=(
                                f"**Address:** `{short_address}`\n"
                                f"**ADA Balance:** `{ada_balance:,.6f} ADA`\n"
                                f"**YUMMI Balance:** `{yummi_balance:,}`\n"
                                f"**YUMMI Warnings:** `{warning_count}/3`\n"
                                f"**Transactions:** `{tx_count}`\n"
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

            await self.send_response(interaction, embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error getting balances: {str(e)}")
            await self.send_response(
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
            
            await self.send_response(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing notification settings: {str(e)}")
            await self.send_response(interaction, "❌ An error occurred while showing your notification settings.", ephemeral=True)

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
        bot.run(token, log_handler=None)  # Disable default discord.py logging
        
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}")
        sys.exit(1)
