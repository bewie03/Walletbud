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
import hmac
import hashlib

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi, ApiUrls
from aiohttp import web

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
    WEBHOOK_CONFIRMATIONS
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
    init_db,
    get_all_monitored_addresses,
    get_addresses_for_stake,
    update_pool_for_stake
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
        self.app.router.add_post('/webhooks/blockfrost', self.handle_webhook)
        self.runner = web.AppRunner(self.app)
        self.port = 49152  # Using a high port number less likely to be in use
        self.monitored_addresses = set()
        self.monitored_stake_addresses = set()
        
    async def setup_hook(self):
        """Setup hook called before the bot starts"""
        try:
            # Initialize database
            await init_db()
            
            # Load monitored addresses
            addresses, stake_addresses = await get_all_monitored_addresses()
            self.monitored_addresses = addresses
            self.monitored_stake_addresses = stake_addresses
            
            logger.info(f"Loaded {len(addresses)} monitored addresses")
            logger.info(f"Loaded {len(stake_addresses)} stake addresses")
            
        except Exception as e:
            logger.error(f"Error in setup: {str(e)}")
            raise e

    async def update_webhook_conditions(self):
        """Update Blockfrost webhook conditions"""
        try:
            # Get Blockfrost API client
            project_id = os.getenv('BLOCKFROST_PROJECT_ID')
            if not project_id:
                logger.error("BLOCKFROST_PROJECT_ID not configured")
                return
                
            # Update transaction webhook
            conditions = {
                "addresses": list(self.monitored_addresses),
                "confirmations": config.WEBHOOKS['transaction']['confirmations']
            }
            await self.rate_limited_request(
                self.blockfrost_client.webhook_update,
                webhook_id=config.WEBHOOKS['transaction']['id'],
                conditions=conditions
            )
            logger.info(f"Updated transaction webhook with {len(self.monitored_addresses)} addresses")
            
            # Update delegation webhook
            conditions = {
                "stake_addresses": list(self.monitored_stake_addresses),
                "confirmations": config.WEBHOOKS['delegation']['confirmations']
            }
            await self.rate_limited_request(
                self.blockfrost_client.webhook_update,
                webhook_id=config.WEBHOOKS['delegation']['id'],
                conditions=conditions
            )
            logger.info(f"Updated delegation webhook with {len(self.monitored_stake_addresses)} stake addresses")
            
        except Exception as e:
            logger.error(f"Error updating webhook conditions: {str(e)}")

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            logger.info("Creating Blockfrost client...")
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_PROJECT_ID
            )
            logger.info("Blockfrost client created successfully")
            
            # Test connection
            logger.info("Testing Blockfrost connection...")
            test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
            logger.info(f"Testing with address: {test_address[:20]}...")
            
            try:
                logger.info("Testing address info endpoint...")
                await self.rate_limited_request(
                    self.blockfrost_client.address,
                    test_address
                )
                logger.info("Blockfrost connection test successful")
                return True
                
            except Exception as e:
                logger.error(f"Address info test failed: {str(e)}")
                logger.error(f"Error details: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to create Blockfrost client: {str(e)}")
            logger.error(f"Error details: {e}")
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

    async def check_yummi_requirement(self, address: str, user_id: int):
        """Check if wallet meets YUMMI token requirement"""
        try:
            assets = await self.rate_limited_request(
                self.blockfrost_client.address_assets,
                address=address
            )
            
            # Find YUMMI token
            yummi_balance = 0
            for asset in assets:
                if asset.unit == f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}":
                    yummi_balance = int(asset.quantity)
                    break
            
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
            return False

    @tasks.loop(seconds=WALLET_CHECK_INTERVAL)
    async def check_wallets(self):
        """Check all registered wallets periodically"""
        while True:
            try:
                wallets = await get_all_wallets()
                if not wallets:
                    await asyncio.sleep(300)
                    continue

                # Process in smaller batches of 25 to stay within rate limits
                # 25 wallets = ~75 calls per batch, allowing for burst recovery
                wallet_batches = [wallets[i:i + 25] for i in range(0, len(wallets), 25)]
                
                for batch_idx, batch in enumerate(wallet_batches):
                    try:
                        addresses = [w['address'] for w in batch]
                        
                        # Get all data concurrently for this batch
                        batch_utxos, batch_txs, batch_extended = await self.get_batch_data(addresses)
                        
                        # Process each wallet in the batch
                        for i, wallet in enumerate(batch):
                            address = wallet['address']
                            user_id = wallet['user_id']
                            
                            # YUMMI check (every 6 hours)
                            last_yummi_check = await get_last_yummi_check(address)
                            if not last_yummi_check or (datetime.now() - last_yummi_check).total_seconds() > 21600:
                                meets_req = await self.check_yummi_requirement(address, user_id)
                                await update_last_yummi_check(address)
                                
                                if not meets_req:
                                    warning_count = await increment_yummi_warning(wallet['id'])
                                    if warning_count >= 3:
                                        logger.warning(f"Removing wallet {address} due to insufficient YUMMI balance")
                                        await remove_wallet(wallet['id'])
                                        continue
                            
                            # Policy check (weekly)
                            last_policy_check = await get_last_policy_check(address)
                            if not last_policy_check or (datetime.now() - last_policy_check).total_seconds() > 604800:
                                await self._check_policy_expiry(address, user_id)
                            
                            # Get wallet's data from batch responses
                            wallet_utxos = [u for u in batch_utxos if u.address == address]
                            wallet_txs = [tx for tx in batch_txs if tx.address == address]
                            wallet_extended = batch_extended[i]
                            
                            # Process regular checks
                            await self._process_wallet_data(
                                address, 
                                user_id, 
                                wallet_utxos, 
                                wallet_txs,
                                wallet_extended
                            )
                            
                            # Get stake address if not cached
                            if not await get_stake_address(address):
                                stake_address = await self.rate_limited_request(
                                    self.blockfrost_client.address_stake,
                                    address
                                )
                                if stake_address:
                                    await update_stake_address(address, stake_address)
                        
                    except Exception as e:
                        logger.error(f"Error processing wallet batch: {str(e)}")
                        if hasattr(e, '__dict__'):
                            logger.error(f"Error details: {e.__dict__}")
                        continue
                    
                    # Calculate delay needed for rate limit
                    # Each batch uses ~75 calls, we want to recharge at least that many
                    # At 10 calls/second recharge rate, we need 7.5 seconds
                    delay = 8 if batch_idx < len(wallet_batches) - 1 else 0
                    await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error in check_wallets loop: {str(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
            
            # Wait 5 minutes before next check
            await asyncio.sleep(300)

    async def get_paginated_transactions(self, address: str, pages: int = 3) -> list:
        """Get paginated transactions for an address
        
        Args:
            address: Wallet address
            pages: Number of pages to fetch (default 3 = 300 transactions)
            
        Returns:
            List of transactions
        """
        all_txs = []
        for page in range(1, pages + 1):
            txs = await self.rate_limited_request(
                self.blockfrost_client.address_transactions,
                address=address,
                count=100,
                page=page
            )
            if not txs:
                break
            all_txs.extend(txs)
        return all_txs

    async def get_batch_data(self, addresses: list) -> tuple:
        """Get all data for a batch of addresses concurrently
        
        Args:
            addresses: List of wallet addresses
            
        Returns:
            Tuple of (utxos, transactions, extended_info)
        """
        # Create tasks for each type of request
        utxo_task = self.rate_limited_request(
            self.blockfrost_client.addresses_utxos,
            addresses=addresses
        )
        
        tx_task = self.rate_limited_request(
            self.blockfrost_client.addresses_transactions,
            addresses=addresses,
            count=100
        )
        
        # Get extended info for each address concurrently
        extended_tasks = [
            self.rate_limited_request(
                self.blockfrost_client.address_extended,
                address=addr
            ) for addr in addresses
        ]
        
        # Run all tasks concurrently
        utxos, txs, extended_info = await asyncio.gather(
            utxo_task,
            tx_task,
            asyncio.gather(*extended_tasks)
        )
        
        return utxos, txs, extended_info

    async def _process_wallet_data(self, address: str, user_id: int, utxos: list, transactions: list, extended_info):
        """Process wallet data from batch responses"""
        try:
            monitoring_since = await get_monitoring_since(address)
            if not monitoring_since:
                return

            # Filter transactions after monitoring started
            new_transactions = [
                tx for tx in transactions 
                if datetime.fromisoformat(tx.block_time) > monitoring_since
            ]

            # Process checks in parallel
            await asyncio.gather(
                self._check_delegation_status(address, user_id, extended_info),
                self._check_balance_changes(address, user_id, utxos, new_transactions)
            )

        except Exception as e:
            logger.error(f"Error processing wallet data for {address}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

    async def _check_balance_changes(self, address: str, user_id: int, utxos: list, transactions: list):
        """Check for balance changes in wallet"""
        try:
            # Get previous UTxO state
            previous_state = await get_utxo_state(address)
                
            # Update UTxO state
            new_state = {
                'utxos': [utxo.tx_hash for utxo in utxos],
                'last_updated': datetime.now().isoformat()
            }

            # Only notify if we have a previous state to compare against
            if previous_state and previous_state.get('utxos'):
                old_utxos = set(previous_state['utxos'])
                new_utxos = set(new_state['utxos'])
                
                # Check for spent and received UTXOs
                spent = old_utxos - new_utxos
                received = new_utxos - old_utxos
                
                if spent or received:
                    # Get transaction details for new UTXOs
                    message = f"💰 Balance change detected in wallet {address[:8]}...\n"
                    
                    if spent:
                        message += "🔸 Outgoing transaction(s)\n"
                    if received:
                        message += "🔹 Incoming transaction(s)\n"
                        
                    await self.notify_user(user_id, message)
            
            # Update the stored state
            await update_utxo_state(address, new_state)

        except Exception as e:
            logger.error(f"Error checking balance changes for {address}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

    async def _check_delegation_status(self, address: str, user_id: int, extended_info):
        """Check delegation status changes"""
        try:
            if not extended_info:
                return
                
            stake_address = extended_info.stake_address
            if not stake_address:
                return
                
            # Get previous delegation status
            prev_status = await get_delegation_status(address)
            curr_status = {
                'pool_id': extended_info.stake_pool_id,
                'active': extended_info.stake_address is not None
            }
            
            # Check for changes
            if prev_status:
                if prev_status['pool_id'] != curr_status['pool_id']:
                    if curr_status['pool_id']:
                        await self.notify_user(
                            user_id,
                            f"🏊‍♂️ Wallet {address[:8]}... has been delegated to a new stake pool: {curr_status['pool_id']}"
                        )
                    else:
                        await self.notify_user(
                            user_id,
                            f"⚠️ Wallet {address[:8]}... is no longer delegated to any stake pool"
                        )
                        
            # Update stored status
            await update_delegation_status(address, curr_status)
            
        except Exception as e:
            logger.error(f"Error checking delegation status for {address}: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

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
            success = await add_wallet_to_db(address, interaction.user.id)
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
            success = await remove_wallet_from_db(address)
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
                    "• `nft` - NFT updates\n"
                    "• `policy` - Policy expiry alerts\n"
                    "• `stake` - Stake key changes\n"
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
                test_address = "addr1qy8ac7qqy0vtulyl7wntmsxc6wex80gvcyjy33qffrhm7sh927ysx5sftuw0dlft05dz3c7revpf527z2zf2p2czd2ps8k9liw"
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
                            if amount.unit.lower() == f"{YUMMI_POLICY_ID}{YUMMI_TOKEN_NAME}".lower()
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
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing notification settings: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while showing your notification settings.", ephemeral=True)

    async def verify_webhook_signature(self, request, event_type: str):
        """Verify Blockfrost webhook signature"""
        try:
            # Get webhook config based on event type
            webhook_config = None
            if event_type == "transaction":
                webhook_config = config.WEBHOOKS["transaction"]
            elif event_type == "delegation":
                webhook_config = config.WEBHOOKS["delegation"]
                
            if not webhook_config:
                logger.error(f"No webhook config found for {event_type}")
                return False
            
            # Get request body
            body = await request.text()
            
            # Get headers
            webhook_id = request.headers.get('Webhook-Id')
            auth_token = request.headers.get('Auth-Token')
            signature = request.headers.get('Signature')
            
            # Log headers for debugging
            logger.debug(f"Webhook verification headers:")
            logger.debug(f"  Webhook-Id: {webhook_id}")
            logger.debug(f"  Auth-Token: {auth_token}")
            logger.debug(f"  Signature: {signature}")
            
            # Check webhook ID
            if webhook_id != webhook_config['id']:
                logger.error(f"Invalid webhook ID. Expected {webhook_config['id']}, got {webhook_id}")
                return False
            
            # Check auth token
            if auth_token != webhook_config['auth_token']:
                logger.error(f"Invalid auth token for {event_type}")
                return False
            
            # Verify signature
            if not signature:
                logger.error(f"Missing signature for {event_type}")
                return False
                
            # Create expected signature
            expected_sig = hmac.new(
                webhook_config['auth_token'].encode(),
                body.encode(),
                hashlib.sha256
            ).hexdigest()
            
            # Compare signatures
            if not hmac.compare_digest(signature, expected_sig):
                logger.error(f"Invalid signature for {event_type}")
                logger.debug(f"Expected signature: {expected_sig}")
                logger.debug(f"Received signature: {signature}")
                return False
            
            logger.info(f"Webhook verification successful for {event_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying webhook signature: {str(e)}")
            logger.error(f"Error details: {e}")
            return False

    async def handle_webhook(self, request):
        """Handle incoming Blockfrost webhooks"""
        try:
            # Parse payload first to get event type
            payload = await request.json()
            event_type = payload.get('event_type')
            
            if not event_type:
                return web.Response(status=400, text="Missing event type")
                
            # Check if event type is supported
            if event_type not in ['transaction', 'delegation']:
                return web.Response(status=400, text="Unsupported event type")
                
            # Verify webhook authentication
            if not await self.verify_webhook_signature(request, event_type):
                return web.Response(status=401, text="Invalid authentication")
            
            # Check confirmations
            confirmations = payload.get('confirmations', 0)
            required_confirmations = config.WEBHOOKS[event_type]['confirmations']
            
            if confirmations < required_confirmations:
                return web.Response(status=202, text="Waiting for more confirmations")
            
            if event_type == 'transaction':
                await self.handle_transaction_webhook(payload)
            elif event_type == 'delegation':
                await self.handle_delegation_webhook(payload)
            
            return web.Response(status=200)
            
        except Exception as e:
            logger.error(f"Error handling webhook: {str(e)}")
            return web.Response(status=500)

    async def handle_transaction_webhook(self, payload):
        """Handle transaction webhook events"""
        try:
            tx_hash = payload.get('tx_hash')
            if not tx_hash:
                logger.error("Missing tx_hash in webhook payload")
                return
                
            # Get full transaction details
            tx_details = await self.rate_limited_request(
                self.blockfrost_client.transaction,
                hash=tx_hash
            )
            
            # Get affected addresses
            affected_addresses = set()
            for utxo in tx_details.utxos:
                for addr in utxo.addresses:
                    if addr in self.monitored_addresses:
                        affected_addresses.add(addr)
            
            # Process transaction for each affected address
            for address in affected_addresses:
                user_id = await get_user_id_for_wallet(address)
                if user_id:
                    await self.process_transaction(address, user_id, tx_details)
                    
        except Exception as e:
            logger.error(f"Error handling transaction webhook: {str(e)}")

    async def handle_delegation_webhook(self, payload):
        """Handle delegation webhook events"""
        try:
            stake_address = payload.get('stake_address')
            if not stake_address or stake_address not in self.monitored_stake_addresses:
                return
                
            pool_id = payload.get('pool_id')
            action = payload.get('action')
            
            # Update stake address pool
            if pool_id:
                await update_pool_for_stake(stake_address, pool_id)
            
            # Get all addresses for this stake address
            addresses = await get_addresses_for_stake(stake_address)
            
            # Get pool info if needed
            pool_info = None
            if pool_id:
                pool_info = await self.rate_limited_request(
                    self.blockfrost_client.pool,
                    pool_id=pool_id
                )
            
            # Notify users
            for address in addresses:
                user_id = await get_user_id_for_wallet(address)
                if user_id:
                    if action == 'delegation_started':
                        message = (
                            f"🏊‍♂️ Wallet {address[:8]}... delegated to "
                            f"{pool_info.metadata.name if pool_info and pool_info.metadata else pool_id}"
                        )
                    else:
                        message = f"⚠️ Wallet {address[:8]}... stopped delegation"
                    
                    await self.notify_user(user_id, message)
                    
        except Exception as e:
            logger.error(f"Error handling delegation webhook: {str(e)}")

    async def update_webhook_conditions(self):
        """Update Blockfrost webhook conditions with current addresses"""
        try:
            # Get Blockfrost API client
            project_id = os.getenv('BLOCKFROST_PROJECT_ID')
            if not project_id:
                logger.error("BLOCKFROST_PROJECT_ID not configured")
                return

            # Update transaction webhook conditions
            tx_webhook_id = os.getenv('BLOCKFROST_TX_WEBHOOK_ID')
            if tx_webhook_id:
                conditions = {
                    "addresses": list(self.monitored_addresses),
                    "confirmations": 2
                }
                await self.rate_limited_request(
                    self.blockfrost_client.webhook_update,
                    webhook_id=tx_webhook_id,
                    conditions=conditions
                )

            # Update delegation webhook conditions
            del_webhook_id = os.getenv('BLOCKFROST_DEL_WEBHOOK_ID')
            if del_webhook_id:
                conditions = {
                    "stake_addresses": list(self.monitored_stake_addresses),
                    "confirmations": 1
                }
                await self.rate_limited_request(
                    self.blockfrost_client.webhook_update,
                    webhook_id=del_webhook_id,
                    conditions=conditions
                )

        except Exception as e:
            logger.error(f"Error updating webhook conditions: {str(e)}")

    async def add_wallet(self, address: str, user_id: int):
        """Add a wallet to monitoring"""
        try:
            # Existing wallet addition logic
            stake_address = await self.get_stake_address(address)
            if not stake_address:
                return False, "Invalid wallet address"

            # Add to database
            success = await add_wallet_to_db(address, user_id, stake_address)
            if not success:
                return False, "Failed to add wallet to database"

            # Add to memory sets for fast lookup
            self.monitored_addresses.add(address)
            self.monitored_stake_addresses.add(stake_address)

            # Update webhook conditions
            await self.update_webhook_conditions()

            return True, "Wallet added successfully"

        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            return False, "Error adding wallet"

    async def remove_wallet(self, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Get stake address before removal
            stake_address = await self.get_stake_address(address)
            
            # Remove from database
            success = await remove_wallet_from_db(address)
            if not success:
                return False

            # Remove from memory sets
            self.monitored_addresses.remove(address)
            
            # Only remove stake address if no other wallets use it
            other_wallets = await get_addresses_for_stake(stake_address)
            if not other_wallets:
                self.monitored_stake_addresses.remove(stake_address)

            # Update webhook conditions
            await self.update_webhook_conditions()

            return True

        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            return False

    async def start(self, *args, **kwargs):
        """Start the bot and webhook server"""
        try:
            # Start webhook server
            await self.runner.setup()
            site = web.TCPSite(
                self.runner, 
                '0.0.0.0',
                self.port
            )
            await site.start()
            logger.info(f"Webhook server started on port {self.port}")
            
            # Initialize Blockfrost client
            await self.init_blockfrost()
            
            # Update webhook conditions
            if self.blockfrost_client:
                await self.update_webhook_conditions()
            
            # Start the bot
            await super().start(*args, **kwargs)
            
        except Exception as e:
            logger.error(f"Error in setup: {str(e)}")
            raise e

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

            @self.tree.command(name="addwallet", description="Add a wallet to monitor")
            @app_commands.describe(address="The wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._add_wallet(interaction, address)

            @self.tree.command(name="removewallet", description="Remove a wallet from monitoring")
            @app_commands.describe(address="The wallet address to stop monitoring")
            async def removewallet(interaction: discord.Interaction, address: str):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._remove_wallet(interaction, address)

            @self.tree.command(name="list", description="List your monitored wallets")
            async def list_wallets(interaction: discord.Interaction):
                if not isinstance(interaction.channel, discord.DMChannel):
                    await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
                    return
                await self._list_wallets(interaction)

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
        bot.run(token, reconnect=True, log_handler=None)  # Disable default discord.py logging
        
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}")
        sys.exit(1)
