import os
import logging
import sys
from datetime import datetime
import time
import uuid
import asyncio

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi

from config import (
    BLOCKFROST_PROJECT_ID,
    BLOCKFROST_BASE_URL,
    MAX_REQUESTS_PER_SECOND,
    BURST_LIMIT,
    RATE_LIMIT_COOLDOWN,
    YUMMI_TOKEN_ID,
    YUMMI_REQUIREMENT,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    WALLET_CHECK_INTERVAL
)

from database import (
    get_pool,
    add_wallet,
    remove_wallet,
    get_user_id_for_wallet,
    get_wallet_for_user,
    get_all_wallets_for_user,
    get_last_yummi_check,
    update_last_yummi_check,
    update_last_checked,
    get_wallet_balance,
    add_transaction,
    get_transaction_metadata,
    get_notification_settings,
    update_notification_setting,
    should_notify,
    get_recent_transactions,
    update_ada_balance,
    update_token_balances,
    update_utxo_state,
    get_stake_address,
    update_stake_address,
    is_reward_processed,
    add_processed_reward,
    get_last_transactions,
    get_utxo_state,
    get_delegation_status,
    update_delegation_status,
    get_dapp_interactions,
    update_dapp_interaction,
    initialize_notification_settings,
    get_yummi_warning_count,
    reset_yummi_warning,
    increment_yummi_warning,
    get_policy_expiry,
    update_policy_expiry,
    init_db,
    get_all_wallets
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
    return str(uuid.uuid4())[:8]

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
        "ada_transactions": "ada_transactions",
        "token_changes": "token_changes", 
        "nft_updates": "nft_updates",
        "staking_rewards": "staking_rewards",
        "stake_changes": "stake_changes",
        "low_balance": "low_balance",
        "policy_expiry": "policy_expiry",
        "delegation_status": "delegation_status",
        "dapp_interactions": "dapp_interactions"
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
            logger.info("Initializing bot...")
            
            # Initialize database
            await init_db()
            logger.info("Database initialized")
            
            # Initialize Blockfrost client
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost client")
                return
                
            # Start wallet monitoring task
            self.check_wallets.start()
            logger.info("Started wallet monitoring task")
            
            # Sync commands
            await self.tree.sync()
            logger.info("Synced commands")
            
        except Exception as e:
            logger.error(f"Error in setup_hook: {str(e)}")
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
            logger.info(f"Using base URL: {BLOCKFROST_BASE_URL}")
            
            # Create client with correct project ID and base URL
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_PROJECT_ID,
                base_url=BLOCKFROST_BASE_URL
            )
            logger.info("BlockFrostApi client created successfully")
            
            # Test connection with address endpoint
            try:
                logger.info("Testing Blockfrost connection...")
                loop = asyncio.get_event_loop()
                # Use a known valid address to test connection
                test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
                logger.info(f"Testing with address: {test_address[:20]}...")
                
                # Test basic address info
                logger.info("Testing address info endpoint...")
                address_info = await loop.run_in_executor(None, 
                    lambda: self.blockfrost_client.address(test_address))
                logger.info(f"Address info response received: {bool(address_info)}")
                
                # Test address total
                logger.info("Testing address total endpoint...")
                total = await loop.run_in_executor(None,
                    lambda: self.blockfrost_client.address_total(test_address))
                logger.info(f"Address total response received: {bool(total)}")
                
                # Test UTXOs
                logger.info("Testing UTXOs endpoint...")
                utxos = await loop.run_in_executor(None,
                    lambda: self.blockfrost_client.address_utxos(test_address))
                logger.info(f"UTXOs response received: {bool(utxos)}")
                
                logger.info("Blockfrost connection test passed")
                return True
                
            except Exception as e:
                logger.error(f"Failed to test Blockfrost connection: {str(e)}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Response details: {e.response.text}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                self.blockfrost_client = None
                return False
            
        except Exception as e:
            logger.error(f"Failed to create Blockfrost client: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response details: {e.response.text}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
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
            logger.error("Blockfrost client not initialized")
            raise RuntimeError("Blockfrost client not initialized")
            
        request_id = get_request_id()
        logger.debug(f"[{request_id}] Making rate-limited request to {func.__name__}")
        
        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                # Acquire rate limit token
                await self.rate_limiter.acquire()
                
                # Make request
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                
                # Release token (no-op but good practice)
                await self.rate_limiter.release()
                
                return result
                
            except Exception as e:
                if attempt < API_RETRY_ATTEMPTS - 1:
                    # Calculate backoff time
                    backoff = (2 ** attempt) * API_RETRY_DELAY
                    logger.warning(
                        f"[{request_id}] Request failed (attempt {attempt + 1}/{API_RETRY_ATTEMPTS}), "
                        f"retrying in {backoff}s: {str(e)}"
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(f"[{request_id}] All retry attempts failed: {str(e)}")
                    raise

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

    async def check_yummi_requirement(self, address: str) -> tuple[bool, int]:
        """Check if wallet meets YUMMI token requirement
        
        Args:
            address (str): Wallet address to check
            
        Returns:
            tuple: (bool, int) - (meets requirement, current balance)
        """
        try:
            # First check UTXOs
            utxos = await self.rate_limited_request(
                self.blockfrost_client.address_utxos,
                address
            )
            
            # Calculate YUMMI amount from UTXOs
            yummi_amount = 0
            for utxo in utxos:
                for amount in utxo.amount:
                    if amount.unit == f"{YUMMI_POLICY_ID}{YUMMI_ASSET_NAME}":
                        yummi_amount += int(amount.quantity)
            
            # If UTXOs show 0, try getting address details directly
            if yummi_amount == 0:
                logger.info(f"UTXOs showed 0 balance for {address}, checking address details...")
                details = await self.rate_limited_request(
                    self.blockfrost_client.address_details,
                    address
                )
                
                # Log all amounts found
                if hasattr(details, 'amount'):
                    for amount in details.amount:
                        logger.info(f"Found token in details for {address}: {amount.unit} = {amount.quantity}")
                    
                    yummi_amount = sum(
                        int(amount.quantity)
                        for amount in details.amount
                        if amount.unit == f"{YUMMI_POLICY_ID}{YUMMI_ASSET_NAME}"
                    )
            
            logger.info(f"Found YUMMI amount for {address}: {yummi_amount}")
            logger.info(f"Required amount: {YUMMI_REQUIREMENT}")
            
            return yummi_amount >= YUMMI_REQUIREMENT, yummi_amount
            
        except Exception as e:
            logger.error(f"Error checking YUMMI requirement: {str(e)}")
            return False, 0

    @tasks.loop()
    async def check_wallets(self):
        """Background task to check all wallets periodically"""
        while True:
            try:
                # Get all wallets to check
                wallets = await get_all_wallets()
                if not wallets:
                    logger.debug("No wallets to check")
                    await asyncio.sleep(WALLET_CHECK_INTERVAL)
                    continue
                
                logger.info(f"Checking {len(wallets)} wallets...")
                
                # Check each wallet
                for wallet in wallets:
                    if not self.monitoring_paused:
                        await self.check_wallet(wallet['address'])
                    
            except Exception as e:
                logger.error(f"Error in wallet check loop: {str(e)}")
                
            finally:
                # Always sleep between checks
                await asyncio.sleep(WALLET_CHECK_INTERVAL)

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
                            await remove_wallet(user_id, address)
                            await self.send_dm(
                                int(user_id),
                                f"❌ Your wallet `{address[:8]}...{address[-8:]}` has been removed due to insufficient YUMMI balance.\n"
                                f"Current balance: {balance:,} YUMMI\n"
                                f"Required balance: {YUMMI_REQUIREMENT:,} YUMMI"
                            )
                            return
                        
                        # Send warning
                        await self.send_dm(
                            int(user_id),
                            f"⚠️ Warning ({warning_count}/3): Your wallet `{address[:8]}...{address[-8:]}` has insufficient YUMMI balance.\n"
                            f"Current balance: {balance:,} YUMMI\n"
                            f"Required balance: {YUMMI_REQUIREMENT:,} YUMMI\n\n"
                            f"Your wallet will be removed after 3 warnings."
                        )

                # Get and update stake address
                stake_address = await get_stake_address(address)
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
                recent_txs = await get_recent_transactions(address)
                if recent_txs:
                    last_known_txs = await get_last_transactions(address) or []
                    
                    for tx in recent_txs:
                        if tx.hash not in last_known_txs:
                            # Add new transaction to database
                            await add_transaction(wallet_id, tx.hash)
                            
                            # Check for staking rewards
                            if not await is_reward_processed(tx.hash):
                                rewards = await self.rate_limited_request(
                                    self.blockfrost_client.transaction_stake_addresses,
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
                    self.blockfrost_client.policy,
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

    async def _check_delegation_status(self, address: str, user_id: int):
        """Check for delegation status changes"""
        try:
            # Get stake address
            addr_details = await self.rate_limited_request(
                self.blockfrost_client.address_details,
                address
            )
            
            if addr_details and addr_details.stake_address:
                # Get delegation info
                delegation = await self.rate_limited_request(
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

    async def _check_dapp_interactions(self, address: str, user_id: int):
        """Check for DApp interactions"""
        try:
            # Get latest transactions
            transactions = await self.rate_limited_request(
                self.blockfrost_client.address_transactions,
                address,
                count=10
            )
            
            # Get last processed tx
            last_processed = await get_dapp_interactions(address)
            
            for tx in transactions:
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

    async def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if we should send a notification to the user
        
        Args:
            user_id (int): Discord user ID
            notification_type (str): Type of notification
            
        Returns:
            bool: Whether to send the notification
        """
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
        """Send token change notification
        
        Args:
            address (str): Wallet address
            user_id (int): Discord user ID
            token_id (str): Token ID
            amount (int): Current amount
            change_type (str): Type of change (new/increase/decrease)
            change_amount (int, optional): Amount changed. Defaults to None.
        """
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
                app_commands.Choice(name="ADA Transactions", value="ada_transactions"),
                app_commands.Choice(name="Token Changes", value="token_changes"),
                app_commands.Choice(name="NFT Updates", value="nft_updates"),
                app_commands.Choice(name="Staking Rewards", value="staking_rewards"),
                app_commands.Choice(name="Stake Key Changes", value="stake_changes"),
                app_commands.Choice(name="Low Balance Alerts", value="low_balance"),
                app_commands.Choice(name="Policy Expiry Alerts", value="policy_expiry"),
                app_commands.Choice(name="Delegation Status", value="delegation_status"),
                app_commands.Choice(name="DApp Interactions", value="dapp_interactions")
            ])
            async def toggle(interaction: discord.Interaction, notification_type: str):
                """Toggle notification settings
                
                Args:
                    interaction (discord.Interaction): Discord interaction
                    notification_type (str): Type of notification to toggle
                """
                try:
                    # Validate notification type
                    if notification_type not in self.NOTIFICATION_TYPES.values():
                        await self.send_response(
                            interaction,
                            "❌ Invalid notification type. Use `/help` to see valid options.",
                            ephemeral=True
                        )
                        return
                        
                    # Get current settings
                    settings = await get_notification_settings(str(interaction.user.id))
                    if not settings:
                        # Initialize settings if they don't exist
                        await initialize_notification_settings(str(interaction.user.id))
                        settings = self.DEFAULT_SETTINGS.copy()
            
                    # Toggle the setting
                    current = settings.get(notification_type, True)
                    success = await update_notification_setting(str(interaction.user.id), notification_type, not current)
                    
                    if success:
                        status = "enabled" if not current else "disabled"
                        await self.send_response(
                            interaction,
                            f"✅ {self.NOTIFICATION_DISPLAY[notification_type]} notifications {status}!",
                            ephemeral=True
                        )
                    else:
                        await self.send_response(
                            interaction,
                            "❌ Failed to update notification settings. Please try again.",
                            ephemeral=True
                        )
                        
                except Exception as e:
                    logger.error(f"Error toggling notification: {str(e)}")
                    await self.send_response(
                        interaction,
                        "❌ An error occurred while updating notification settings.",
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
            wallet = await get_wallet_for_user(address)
            if not wallet or wallet['user_id'] != str(interaction.user.id):
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
            addresses = await get_all_wallets_for_user(str(interaction.user.id))
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
                await self.rate_limited_request(
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
            # Get user's wallets
            wallets = await get_all_wallets_for_user(str(interaction.user.id))
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
