import os
import logging
import sys
from datetime import datetime, timedelta
from uuid import uuid4
import time

import discord
from discord import app_commands
from discord.ext import commands
from blockfrost import BlockFrostApi
import asyncio

from config import (
    BLOCKFROST_PROJECT_ID,
    MAX_REQUESTS_PER_SECOND, BURST_LIMIT, RATE_LIMIT_COOLDOWN,
    WALLET_CHECK_INTERVAL,
    YUMMI_TOKEN_ID, YUMMI_REQUIREMENT,
    API_RETRY_ATTEMPTS, API_RETRY_DELAY
)
from database import (
    init_db,
    add_wallet,
    remove_wallet,
    get_wallet,
    get_all_wallets,
    update_last_checked,
    get_user_id_for_wallet,
    is_reward_processed,
    add_processed_reward,
    get_stake_address,
    update_stake_address,
    get_notification_settings,
    check_ada_balance,
    get_all_wallets_for_user,
    get_wallet_id,
    get_last_yummi_check,
    update_last_yummi_check,
    get_yummi_warning_count,
    increment_yummi_warning,
    reset_yummi_warning,
    get_policy_expiry,
    update_policy_expiry,
    get_delegation_status,
    update_delegation_status,
    get_dapp_interactions,
    update_dapp_interaction,
    update_notification_setting,
    get_last_dapp_tx,
    update_last_dapp_tx,
    add_transaction,
    initialize_notification_settings
)

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logger.info(f"Starting bot with log level: {log_level}")

# Create request ID for logging
def get_request_id():
    return str(uuid4())[:8]

def dm_only():
    """Check if command is being used in DMs"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(
                "❌ This command can only be used in DMs.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

def has_blockfrost(func=None):
    """Check if Blockfrost client is available"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.client.blockfrost_client:
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            return False
        return True
    
    if func is None:
        return app_commands.check(predicate)
    return app_commands.check(predicate)(func)

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
        self.requests = []
        self.burst_tokens = burst_limit
        self.last_burst_update = time.time()
        self.lock = asyncio.Lock()
        
        logger.info(
            f"Initialized RateLimiter with: max_requests={max_requests}, "
            f"burst_limit={burst_limit}, cooldown_seconds={cooldown_seconds}"
        )

    async def acquire(self):
        """Acquire a rate limit token, implementing Blockfrost's burst behavior"""
        async with self.lock:
            now = time.time()
            
            # Remove requests older than 1 second
            cutoff = now - 1
            old_count = len(self.requests)
            self.requests = [t for t in self.requests if t > cutoff]
            if old_count - len(self.requests) > 0:
                logger.debug(f"Cleaned up {old_count - len(self.requests)} old requests")
            
            # Replenish burst tokens at base rate
            time_passed = now - self.last_burst_update
            tokens_to_add = min(
                self.burst_limit - self.burst_tokens,  # Don't exceed burst limit
                int(time_passed * self.max_requests)   # Add tokens at base rate
            )
            
            if tokens_to_add > 0:
                logger.debug(f"Replenishing {tokens_to_add} burst tokens")
                
            self.burst_tokens = min(self.burst_limit, self.burst_tokens + tokens_to_add)
            self.last_burst_update = now

            # Check if we can proceed
            if len(self.requests) >= self.max_requests and self.burst_tokens <= 0:
                # We're at base rate limit and no burst tokens available
                wait_time = self.requests[0] - now + 1
                if wait_time > 0:
                    logger.debug(f"Rate limited, waiting {wait_time:.2f}s (requests={len(self.requests)}, burst_tokens={self.burst_tokens})")
                    await asyncio.sleep(wait_time)
                    return await self.acquire()
            
            # Use a burst token if we're over base rate
            if len(self.requests) >= self.max_requests:
                self.burst_tokens -= 1
                logger.debug(f"Using burst token, {self.burst_tokens} remaining")
            
            self.requests.append(now)
            
            # Log current state periodically
            if len(self.requests) % 100 == 0:
                logger.info(
                    f"Rate limiter status: requests_in_window={len(self.requests)}, "
                    f"burst_tokens={self.burst_tokens}"
                )
            
    async def release(self):
        """Release is a no-op since we use time-based windowing"""
        pass

class WalletBud(commands.Bot):
    """WalletBud Discord bot"""
    NOTIFICATION_TYPES = {
        # Command value -> Database key
        "ada": "ada_transactions",
        "token": "token_changes", 
        "nft": "nft_updates",
        "staking": "staking_rewards",
        "stake": "stake_changes",
        "balance": "low_balance",
        "policy": "policy_expiry",
        "delegation": "delegation_status",
        "dapp": "dapp_interactions"
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
        "dapp_interactions": "DApp Interactions"
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
        "dapp_interactions": True
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
            max_requests=9,  # Slightly lower for safety
            burst_limit=500,
            cooldown_seconds=50
        )
        logger.info(f"Initialized RateLimiter with: max_requests=9, burst_limit=500, cooldown_seconds=50")
        
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
            # Initialize database
            await init_db()
            logger.info("Database initialized")
            
            # Initialize Blockfrost client
            await self.init_blockfrost()
            logger.info("Blockfrost client initialized")
            
            # Set up commands
            logger.info("Setting up commands...")
            await self.setup_commands()
            logger.info("Commands setup complete")
            
            # Start wallet monitoring task
            self.bg_task = self.loop.create_task(self.check_wallets())
            logger.info("Started wallet monitoring task")
            
            logger.info("Bot initialization complete!")
            
        except Exception as e:
            logger.error(f"Error in setup: {str(e)}")
            raise
            
    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            logger.info("Creating Blockfrost client...")
            
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
            
            # Test connection with address endpoint
            try:
                logger.info("Testing Blockfrost connection...")
                loop = asyncio.get_event_loop()
                # Use a known valid address to test connection
                test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
                logger.info(f"Testing with address: {test_address[:20]}...")
                
                # Test basic address info
                address_info = await loop.run_in_executor(None, 
                    lambda: self.blockfrost_client.address(test_address))
                logger.info(f"Address info: {address_info}")
                
                # Test address total
                total = await loop.run_in_executor(None,
                    lambda: self.blockfrost_client.address_total(test_address))
                logger.info(f"Address total: {total}")
                
                # Test UTXOs
                utxos = await loop.run_in_executor(None,
                    lambda: self.blockfrost_client.address_utxos(test_address))
                logger.info(f"Address UTXOs: {utxos[:2]}")  # Show first 2 UTXOs
                
                logger.info("Blockfrost connection test passed")
                return True
                
            except Exception as e:
                logger.error(f"Failed to test Blockfrost connection: {str(e)}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Response details: {e.response.text}")
                self.blockfrost_client = None
                return False
            
        except Exception as e:
            logger.error(f"Failed to create Blockfrost client: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response details: {e.response.text}")
            self.blockfrost_client = None
            return False

    async def rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited request to the Blockfrost API with retry logic
        
        Args:
            func: The Blockfrost API function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            The result of the API call
            
        Raises:
            Exception: If all retry attempts fail
        """
        if not self.blockfrost_client:
            raise RuntimeError("Blockfrost client not initialized")
            
        last_error = None
        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                # First acquire rate limit token
                await self.rate_limiter.acquire()
                
                # Make the API call
                try:
                    loop = asyncio.get_event_loop()
                    if args and not kwargs:
                        result = await loop.run_in_executor(None, func, *args)
                    elif kwargs and not args:
                        result = await loop.run_in_executor(None, lambda: func(**kwargs))
                    else:
                        result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                    
                    # If we get here, the call succeeded
                    return result
                    
                except Exception as e:
                    last_error = e
                    # Check if error is retryable
                    if hasattr(e, 'status_code'):
                        # Don't retry auth errors or invalid requests
                        if e.status_code in [401, 403, 400]:
                            logger.error(f"Non-retryable API error: {str(e)}")
                            raise
                        # If rate limited, wait longer
                        elif e.status_code == 429:
                            retry_delay = API_RETRY_DELAY * (4 ** attempt)  # Wait even longer for rate limits
                            logger.warning(f"Rate limit hit, backing off for {retry_delay:.2f}s")
                        else:
                            retry_delay = API_RETRY_DELAY * (2 ** attempt)
                            logger.warning(f"API error (status={e.status_code}): {str(e)}")
                    else:
                        # For network errors, use standard backoff
                        retry_delay = API_RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"Network error: {str(e)}")
                    
                    # Log retry attempt
                    if attempt < API_RETRY_ATTEMPTS - 1:
                        logger.info(f"Retrying in {retry_delay:.2f}s (attempt {attempt + 1}/{API_RETRY_ATTEMPTS})")
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error(f"All retry attempts failed: {str(last_error)}")
                        raise last_error
                        
            finally:
                await self.rate_limiter.release()

    async def _rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited request with retry logic
        
        Args:
            func: The API function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            The API response
            
        Raises:
            Exception if all retries fail
        """
        retry_count = 0
        max_retries = 3
        rate_limit_hits = 0
        
        while retry_count < max_retries:
            try:
                return await self.rate_limited_request(func, *args, **kwargs)
            except Exception as e:
                retry_count += 1
                rate_limit_hit = "rate limit" in str(e).lower()
                
                if rate_limit_hit:
                    rate_limit_hits += 1
                    if rate_limit_hits >= 3:
                        await self.send_admin_alert(
                            f"⚠️ Frequent rate limits hit for {func.__name__}\n"
                            f"Consider adjusting rate limiter settings."
                        )
                
                if retry_count == max_retries:
                    if isinstance(e, Exception) and "rate limit" not in str(e).lower():
                        await self.send_admin_alert(
                            f"❌ API request failed after {max_retries} retries:\n"
                            f"Function: {func.__name__}\n"
                            f"Error: {str(e)}"
                        )
                    raise e
                    
                # Exponential backoff
                wait_time = 2 ** retry_count
                logger.warning(f"API request failed, retrying in {wait_time}s: {str(e)}")
                await asyncio.sleep(wait_time)

    async def send_admin_alert(self, message: str, is_error: bool = True):
        """Send alert to admin channel
        
        Args:
            message (str): Alert message
            is_error (bool, optional): Whether this is an error alert. Defaults to True.
        """
        if not self.admin_channel_id:
            logger.warning("No admin channel configured for alerts")
            return
            
        try:
            channel = self.get_channel(int(self.admin_channel_id))
            if channel:
                emoji = "🚨" if is_error else "ℹ️"
                await channel.send(f"{emoji} **Alert**: {message}")
            else:
                logger.error(f"Could not find admin channel: {self.admin_channel_id}")
        except Exception as e:
            logger.error(f"Failed to send admin alert: {str(e)}")

    async def check_yummi_requirement(self, address: str):
        """Check if wallet meets YUMMI token requirement
        
        Args:
            address (str): Wallet address to check
            
        Returns:
            tuple: (bool, int) - (meets requirement, current balance)
        """
        try:
            # First try getting UTXOs
            utxos = await self._rate_limited_request(
                self.blockfrost_client.address_utxos,
                address
            )
            
            # Calculate YUMMI balance from UTXOs
            logger.info(f"Checking YUMMI balance for address {address}")
            logger.info(f"YUMMI Token ID: {YUMMI_TOKEN_ID}")
            yummi_amount = sum(
                int(amount.quantity)
                for utxo in utxos
                for amount in utxo.amount
                if amount.unit.lower() == YUMMI_TOKEN_ID.lower()  # Case-insensitive comparison
            )
            
            # If UTXOs show 0, try getting address assets directly
            if yummi_amount == 0:
                logger.info("UTXOs showed 0 balance, checking address assets directly...")
                assets = await self._rate_limited_request(
                    self.blockfrost_client.address_assets,
                    address
                )
                yummi_amount = sum(
                    int(asset.quantity)
                    for asset in assets
                    if asset.unit.lower() == YUMMI_TOKEN_ID.lower()
                )
            
            logger.info(f"Found YUMMI amount: {yummi_amount}")
            return yummi_amount >= YUMMI_REQUIREMENT, yummi_amount
            
        except Exception as e:
            logger.error(f"Error checking YUMMI requirement: {str(e)}")
            return False, 0

    async def check_wallets(self):
        """Background task to check all wallets periodically"""
        await self.wait_until_ready()
        
        while not self.is_closed():
            try:
                # Get all wallets
                wallets = await get_all_wallets()
                if not wallets:
                    logger.debug("No wallets to check")
                    await asyncio.sleep(WALLET_CHECK_INTERVAL)
                    continue
                
                logger.info(f"Checking {len(wallets)} wallets...")
                
                # Process wallets with concurrency limit
                async with asyncio.TaskGroup() as tg:
                    for wallet in wallets:
                        tg.create_task(self.check_wallet(wallet['address']))
            
            except Exception as e:
                logger.error(f"Error in check_wallets task: {str(e)}")
                
            finally:
                # Always sleep between checks
                await asyncio.sleep(WALLET_CHECK_INTERVAL)

    async def check_wallet(self, address: str):
        """Check a single wallet's balance and transactions"""
        try:
            async with self.wallet_task_lock:
                # Get user ID for this wallet
                user_id = await get_user_id_for_wallet(address)
                if not user_id:
                    logger.error(f"No user found for wallet {address}")
                    return

                # Check YUMMI requirement every 6 hours
                last_check = await get_last_yummi_check(address)
                if not last_check or (datetime.now() - last_check).total_seconds() > 21600:  # 6 hours
                    meets_req, balance = await self.check_yummi_requirement(address)
                    await update_last_yummi_check(address)
                    
                    if not meets_req:
                        # Get wallet ID for warning tracking
                        wallet_id = await get_wallet_id(str(user_id), address)
                        if not wallet_id:
                            logger.error(f"Could not find wallet ID for {address}")
                            return

                        # Increment warning count
                        warning_count = await increment_yummi_warning(wallet_id)
                        
                        # Remove wallet after 3 warnings
                        if warning_count >= 3:
                            await remove_wallet(user_id, address)
                            try:
                                user = await self.fetch_user(int(user_id))
                                if user:
                                    await user.send(
                                        f"❌ **Wallet Removed**\n"
                                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                        f"Reason: Insufficient YUMMI tokens after 3 warnings\n"
                                        f"Current YUMMI: `{balance:,}`\n"
                                        f"Required: `{YUMMI_REQUIREMENT:,}`"
                                    )
                            except Exception as e:
                                logger.error(f"Error notifying user about wallet removal: {str(e)}")
                            return
                        else:
                            # Send warning message
                            try:
                                user = await self.fetch_user(int(user_id))
                                if user:
                                    await user.send(
                                        f"⚠️ **YUMMI Balance Warning ({warning_count}/3)**\n"
                                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                        f"Your YUMMI balance is too low!\n"
                                        f"Current YUMMI: `{balance:,}`\n"
                                        f"Required: `{YUMMI_REQUIREMENT:,}`\n\n"
                                        f"Your wallet will be removed after {3-warning_count} more warning{'s' if 3-warning_count > 1 else ''}. "
                                        f"Please restore your YUMMI balance."
                                    )
                            except Exception as e:
                                logger.error(f"Error sending warning message: {str(e)}")
                    else:
                        # Reset warnings if requirement is met
                        wallet_id = await get_wallet_id(str(user_id), address)
                        if wallet_id:
                            await reset_yummi_warning(wallet_id)

                # Get user for notifications
                try:
                    user = await self.fetch_user(int(user_id))
                    if not user:
                        logger.error(f"Could not find user {user_id}")
                        return
                except Exception as e:
                    logger.error(f"Error getting user: {str(e)}")
                    return

                # Get wallet ID
                wallet_id = await get_wallet_id(str(user_id), address)
                if not wallet_id:
                    logger.error(f"Could not find wallet ID for {address}")
                    return

                # Update last checked timestamp
                await update_last_checked(wallet_id)

                # Get current balances
                try:
                    # Get UTXOs for balance calculation
                    utxos = await self._rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    # Calculate ADA balance
                    current_balance = sum(
                        sum(int(amount.quantity) for amount in utxo.amount if amount.unit == 'lovelace')
                        for utxo in utxos
                    ) / 1_000_000  # Convert to ADA
                    
                    # Calculate token balances
                    current_tokens = {}
                    for utxo in utxos:
                        for amount in utxo.amount:
                            if amount.unit != 'lovelace':
                                current_tokens[amount.unit] = current_tokens.get(amount.unit, 0) + int(amount.quantity)
                except Exception as e:
                    logger.error(f"Error getting wallet balances: {str(e)}")
                    return

                # Check for balance changes
                if await self.should_notify(int(user_id), 'balance'):
                    previous_balance = await check_ada_balance(address)
                    if previous_balance is not None and abs(current_balance - previous_balance) > 1:  # 1 ADA threshold
                        change = current_balance - previous_balance
                        direction = "received" if change > 0 else "sent"
                        await user.send(
                            f"💰 **Balance Change Detected!**\n"
                            f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                            f"{direction.title()}: `{abs(change):,.2f} ADA`\n"
                            f"New Balance: `{current_balance:,.2f} ADA`"
                        )

                # Check for token changes
                if await self.should_notify(int(user_id), 'tokens'):
                    previous_tokens = await get_new_tokens(address)
                    if previous_tokens is not None:
                        # Check for new tokens
                        for token_id, amount in current_tokens.items():
                            if token_id not in previous_tokens:
                                token_name = token_id.encode('utf-8').hex()[-8:]  # Get last 4 bytes of policy ID
                                await user.send(
                                    f"🪙 **New Token Detected!**\n"
                                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                    f"Token: `{token_name}`\n"
                                    f"Amount: `{amount:,}`"
                                )
                            elif amount > previous_tokens[token_id]:
                                token_name = token_id.encode('utf-8').hex()[-8:]
                                change = amount - previous_tokens[token_id]
                                await user.send(
                                    f"🪙 **Token Balance Increased!**\n"
                                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                    f"Token: `{token_name}`\n"
                                    f"Received: `{change:,}`\n"
                                    f"New Balance: `{amount:,}`"
                                )
                            elif amount < previous_tokens[token_id]:
                                token_name = token_id.encode('utf-8').hex()[-8:]
                                change = previous_tokens[token_id] - amount
                                await user.send(
                                    f"🪙 **Token Balance Decreased!**\n"
                                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                    f"Token: `{token_name}`\n"
                                    f"Sent: `{change:,}`\n"
                                    f"New Balance: `{amount:,}`"
                                )

                # Update stored balances
                await update_ada_balance(address, current_balance)
                await update_token_balances(address, current_tokens)

                # New checks
                await self._check_policy_expiry(address, user)
                await self._check_delegation_status(address, user)
                await self._check_dapp_interactions(address, user)
                
        except Exception as e:
            logger.error(f"Error checking wallet: {str(e)}")

    async def _check_policy_expiry(self, address: str, user: discord.User):
        """Check for expiring token policies"""
        try:
            # Get all tokens in wallet
            assets = await self._rate_limited_request(
                self.blockfrost_client.address_assets,
                address
            )
            
            for asset in assets:
                policy_id = asset.unit[:56]  # First 56 chars are policy ID
                
                # Get policy script
                try:
                    policy = await self._rate_limited_request(
                        self.blockfrost_client.script,
                        policy_id
                    )
                    
                    if hasattr(policy, 'timelock'):
                        expiry_slot = int(policy.timelock.slot)
                        stored_expiry = await get_policy_expiry(policy_id)
                        
                        if stored_expiry != expiry_slot:
                            await update_policy_expiry(policy_id, expiry_slot)
                            
                            # Get current slot
                            tip = await self._rate_limited_request(
                                self.blockfrost_client.block_latest
                            )
                            current_slot = tip.slot
                            
                            # Alert if policy expires within ~5 days (432000 slots)
                            if expiry_slot - current_slot < 432000:
                                days_left = (expiry_slot - current_slot) // 86400  # ~86400 slots per day
                                if await self.should_notify(int(user.id), "policy_expiry"):
                                    await user.send(
                                        f"⚠️ **Token Policy Expiring Soon!**\n"
                                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                        f"Policy ID: `{policy_id}`\n"
                                        f"Days until expiry: `{days_left}`"
                                    )
                except Exception as e:
                    logger.error(f"Error checking policy {policy_id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error checking policy expiry: {str(e)}")

    async def _check_delegation_status(self, address: str, user: discord.User):
        """Check for delegation status changes"""
        try:
            # Get stake address
            addr_details = await self._rate_limited_request(
                self.blockfrost_client.address_details,
                address
            )
            
            if addr_details and addr_details.stake_address:
                # Get delegation info
                delegation = await self._rate_limited_request(
                    self.blockfrost_client.account_delegation_history,
                    addr_details.stake_address,
                    count=1,
                    page=1
                )
                
                if delegation:
                    current_pool = delegation[0].pool_id
                    stored_pool = await get_delegation_status(address)
                    
                    if current_pool != stored_pool:
                        await update_delegation_status(address, current_pool)
                        
                        if await self.should_notify(int(user.id), "delegation"):
                            pool_info = await self._rate_limited_request(
                                self.blockfrost_client.pool,
                                current_pool
                            )
                            pool_name = pool_info.metadata.name if pool_info.metadata else "Unknown Pool"
                            
                            await user.send(
                                f"🔄 **Delegation Status Changed**\n"
                                f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                f"New Pool: `{pool_name}`\n"
                                f"Pool ID: `{current_pool}`"
                            )
                            
        except Exception as e:
            logger.error(f"Error checking delegation status: {str(e)}")

    async def _check_dapp_interactions(self, address: str, user: discord.User):
        """Check for DApp interactions"""
        try:
            # Get last processed transaction
            last_tx = await get_last_dapp_tx(address)
            
            # Get recent transactions
            txs = await self._rate_limited_request(
                self.blockfrost_client.address_transactions,
                address,
                params={'order': 'desc', 'count': 20}
            )
            
            for tx in txs:
                # Skip if we've already processed this tx
                if last_tx and tx.tx_hash == last_tx:
                    break
                    
                # Get transaction details with retry
                tx_details = None
                retry_count = 0
                while retry_count < 3:
                    try:
                        tx_details = await self._rate_limited_request(
                            self.blockfrost_client.transaction_metadata,
                            tx.tx_hash
                        )
                        break
                    except Exception as e:
                        retry_count += 1
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                
                if not tx_details:
                    logger.error(f"Failed to get transaction details after retries: {tx.tx_hash}")
                    continue
                
                # Check for contract interactions (metadata with known DApp identifiers)
                if tx_details.metadata:
                    # Convert metadata to string for easier searching
                    metadata_str = str(tx_details.metadata).lower()
                    
                    # First check common metadata fields
                    found_fields = []
                    for field in self.metadata_fields:
                        if field in metadata_str:
                            found_fields.append(field)
                            logger.info(f"Found DApp metadata field: {field} in tx {tx.tx_hash}")
                    
                    # Then check specific DApp identifiers
                    for dapp_name, patterns in self.dapp_patterns.items():
                        if any(pattern in metadata_str for pattern in patterns):
                            # Store transaction metadata
                            wallet_id = await get_wallet_id(str(user.id), address)
                            if wallet_id:
                                # Store with found metadata fields
                                metadata = tx_details.metadata.copy()
                                metadata['detected_fields'] = found_fields
                                await add_transaction(
                                    wallet_id=wallet_id,
                                    tx_hash=tx.tx_hash,
                                    metadata=metadata
                                )
                            
                            # Send notification if enabled
                            if await self.should_notify(int(user.id), "dapp_interactions"):
                                await user.send(
                                    f"🔗 **DApp Interaction Detected**\n"
                                    f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                    f"DApp: `{dapp_name}`\n"
                                    f"Transaction: `{tx.tx_hash}`"
                                )
                            break  # Only notify once per transaction
                
                # Update last processed
                await update_last_dapp_tx(address, tx.tx_hash)
                
        except Exception as e:
            logger.error(f"Error checking DApp interactions: {str(e)}")
            await self.send_admin_alert(
                f"Error checking DApp interactions for wallet {address[:8]}...{address[-8:]}: {str(e)}"
            )

    async def should_notify(self, user_id: int, notification_type: str):
        """Check if we should send a notification to the user
        
        Args:
            user_id (int): Discord user ID
            notification_type (str): Type of notification
            
        Returns:
            bool: Whether to send the notification
        """
        try:
            settings = await get_notification_settings(str(user_id))
            if not settings:
                return self.DEFAULT_SETTINGS.get(notification_type, True)
            return settings.get(notification_type, True)
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            return True

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

    async def setup_commands(self):
        """Set up bot commands"""
        try:
            # Wallet management commands
            @self.tree.command(name="addwallet", description="Register a wallet to monitor")
            @app_commands.describe(address="The wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                await self._add_wallet(interaction, address)

            @self.tree.command(name="removewallet", description="Stop monitoring a wallet")
            @app_commands.describe(address="The wallet address to remove")
            async def removewallet(interaction: discord.Interaction, address: str):
                await self._remove_wallet(interaction, address)

            @self.tree.command(name="listwallets", description="List your registered wallets")
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
            async def balance(interaction: discord.Interaction):
                await self._balance(interaction)

            @self.tree.command(name="notifications", description="View your notification settings")
            async def notifications(interaction: discord.Interaction):
                await self._notifications(interaction)

            @self.tree.command(name="toggle", description="Toggle notification types")
            @app_commands.choices(notification_type=[
                app_commands.Choice(name="ADA Transactions", value="ada"),
                app_commands.Choice(name="Token Changes", value="token"),
                app_commands.Choice(name="NFT Updates", value="nft"),
                app_commands.Choice(name="Staking Rewards", value="staking"),
                app_commands.Choice(name="Stake Key Changes", value="stake"),
                app_commands.Choice(name="Low Balance Alerts", value="balance"),
                app_commands.Choice(name="Policy Expiry Alerts", value="policy"),
                app_commands.Choice(name="Delegation Changes", value="delegation"),
                app_commands.Choice(name="DApp Interactions", value="dapp")
            ])
            async def toggle(interaction: discord.Interaction, notification_type: str):
                """Toggle notification settings
                
                Args:
                    interaction (discord.Interaction): Discord interaction
                    notification_type (str): Type of notification to toggle
                """
                if notification_type not in self.NOTIFICATION_TYPES:
                    await self.send_response(
                        interaction,
                        "❌ Invalid notification type. Use `/help` to see valid options.",
                        ephemeral=True
                    )
                    return
                    
                setting_key = self.NOTIFICATION_TYPES[notification_type]
                
                # Get current settings
                settings = await get_notification_settings(str(interaction.user.id))
                if not settings:
                    await update_notification_setting(str(interaction.user.id), setting_key, True)
                    settings = self.DEFAULT_SETTINGS.copy()
        
                # Toggle the setting
                current = settings.get(setting_key, True)
                await update_notification_setting(str(interaction.user.id), setting_key, not current)
        
                status = "enabled" if not current else "disabled"
                await self.send_response(
                    interaction,
                    f"✅ {self.NOTIFICATION_DISPLAY[setting_key]} notifications {status}!",
                    ephemeral=True
                )

            # Sync the commands
            await self.tree.sync()
            logger.info("Commands synced successfully")

        except Exception as e:
            logger.error(f"Failed to set up commands: {str(e)}")
            raise

    async def _add_wallet(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            # Validate address format
            if not address.startswith('addr1'):
                await interaction.response.send_message("❌ Invalid wallet address! Address must start with 'addr1'", ephemeral=True)
                return
                
            # Add wallet to database
            if await add_wallet(str(interaction.user.id), address):
                # Initialize notification settings for new user
                await initialize_notification_settings(str(interaction.user.id))
                
                await interaction.response.send_message(f"✅ Added wallet `{address[:8]}...{address[-8:]}` to tracking", ephemeral=True)
                
                # Start monitoring immediately
                asyncio.create_task(self._monitor_wallet(address, interaction.user))
            else:
                await interaction.response.send_message("❌ Failed to add wallet or wallet already exists", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while adding the wallet", ephemeral=True)

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists
            wallet = await get_wallet(str(interaction.user.id), address)
            if not wallet:
                await interaction.response.send_message("❌ Wallet not found!", ephemeral=True)
                return
            
            # Get wallet ID for cleanup
            wallet_id = await get_wallet_id(str(interaction.user.id), address)
            if wallet_id:
                await reset_yummi_warning(wallet_id)
            
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
            addresses = await get_all_wallets_for_user(str(interaction.user.id))
            if not addresses:
                await interaction.response.send_message("❌ You don't have any registered wallets! Use `/addwallet` first.", ephemeral=True)
                return
            
            # Create embed
            embed = discord.Embed(
                title="📋 Your Registered Wallets",
                description=f"You have {len(addresses)} registered wallet(s):",
                color=discord.Color.blue()
            )
            
            # Add each wallet with its details
            for i, address in enumerate(addresses, 1):
                try:
                    # Get wallet info
                    utxos = await self._rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    if utxos:
                        # Calculate ADA balance
                        ada_balance = sum(
                            int(utxo.amount[0].quantity) / 1_000_000 
                            for utxo in utxos 
                            if utxo.amount and utxo.amount[0].unit == 'lovelace'
                        )
                        
                        # Calculate YUMMI balance
                        yummi_balance = sum(
                            int(amount.quantity)
                            for utxo in utxos
                            for amount in utxo.amount
                            if amount.unit.lower() == YUMMI_TOKEN_ID.lower()
                        )
                        
                        # Get transaction count
                        tx_info = await self._rate_limited_request(
                            self.blockfrost_client.address_total,
                            address
                        )
                        tx_count = tx_info.tx_count if tx_info else 0
                        
                        # Get warning count
                        wallet_id = await get_wallet_id(str(interaction.user.id), address)
                        warning_count = await get_yummi_warning_count(wallet_id) if wallet_id else 0
                        
                        # Format address for display
                        short_address = f"{address[:8]}...{address[-8:]}"
                        
                        # Add wallet field
                        embed.add_field(
                            name=f"🏦 Wallet #{i}",
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
                    logger.error(f"Error getting wallet info: {str(e)}")
                    embed.add_field(
                        name=f"Wallet #{i}",
                        value="❌ Failed to get wallet info",
                        inline=False
                    )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while listing your wallets.", ephemeral=True)

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
                    "`/notifications` - View your notification settings\n"
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
                await self._rate_limited_request(
                    self.blockfrost_client.address,
                    test_address
                )
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
            # Get all user's wallets
            addresses = await get_all_wallets_for_user(str(interaction.user.id))
            if not addresses:
                await interaction.response.send_message("❌ You don't have any registered wallets! Use `/addwallet` first.", ephemeral=True)
                return
            
            # Create embed
            embed = discord.Embed(
                title="💰 Wallet Balances",
                description="Your current wallet balances:",
                color=discord.Color.green()
            )
            
            total_ada = 0
            
            # Get balance for each wallet
            for address in addresses:
                try:
                    # Get current UTXOs
                    utxos = await self._rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    if utxos:
                        # Calculate ADA balance from UTXOs
                        ada_balance = sum(
                            int(utxo.amount[0].quantity) / 1_000_000 
                            for utxo in utxos 
                            if utxo.amount and utxo.amount[0].unit == 'lovelace'
                        )
                        total_ada += ada_balance
                        
                        # Get warning count
                        wallet_id = await get_wallet_id(str(interaction.user.id), address)
                        warning_count = await get_yummi_warning_count(wallet_id) if wallet_id else 0
                        
                        # Calculate YUMMI balance
                        yummi_amount = sum(
                            int(amount.quantity)
                            for utxo in utxos
                            for amount in utxo.amount
                            if amount.unit.lower() == YUMMI_TOKEN_ID.lower()
                        )
                        
                        # Add wallet info
                        embed.add_field(
                            name=f"Wallet `{address[:8]}...{address[-8:]}`",
                            value=(
                                f"Balance: `{ada_balance:,.2f} ADA`\n"
                                f"YUMMI: `{yummi_amount:,}`\n"
                                f"YUMMI Warnings: `{warning_count}/3`"
                            ),
                            inline=False
                        )
                        
                except Exception as e:
                    logger.error(f"Error getting balance for {address}: {str(e)}")
                    embed.add_field(
                        name=f"Wallet `{address[:8]}...{address[-8:]}`",
                        value="❌ Failed to get balance",
                        inline=False
                    )
            
            # Add total balance
            embed.add_field(
                name="Total ADA Balance",
                value=f"`{total_ada:,.2f} ADA`",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while getting your balance.", ephemeral=True)

    async def _notifications(self, interaction: discord.Interaction):
        """View your notification settings"""
        try:
            settings = await get_notification_settings(str(interaction.user.id))
            if not settings:
                settings = self.DEFAULT_SETTINGS.copy()
            
            embed = discord.Embed(
                title="🔔 Notification Settings",
                description="Your current notification settings:",
                color=discord.Color.blue()
            )
            
            for setting_key, display_name in self.NOTIFICATION_DISPLAY.items():
                status = "✅ Enabled" if settings.get(setting_key, True) else "❌ Disabled"
                embed.add_field(
                    name=display_name,
                    value=status,
                    inline=True
                )
            
            embed.add_field(
                name="\u200b",
                value=(
                    "\nUse `/toggle <type>` to enable/disable notifications.\n"
                    "Example: `/toggle ada` to toggle ADA transaction notifications."
                ),
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error getting notification settings: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred while getting your notification settings.",
                ephemeral=True
            )

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
