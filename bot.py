import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from uuid import uuid4
import time

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi
import aiohttp

from config import *
from database import (
    init_db,
    add_wallet,
    remove_wallet,
    get_wallet,
    get_all_wallets,
    update_last_checked,
    add_transaction,
    get_user_id_for_wallet,
    get_utxo_state,
    store_utxo_state,
    is_reward_processed,
    add_processed_reward,
    get_stake_address,
    update_stake_address,
    get_notification_settings,
    update_notification_setting,
    get_wallet_for_user,
    get_wallet_balance,
    is_token_change_processed,
    add_processed_token_change,
    get_recent_transactions,
    get_new_tokens,
    get_removed_nfts,
    check_ada_balance,
    get_all_wallets_for_user
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
    """Rate limiter for API requests"""
    def __init__(self, max_requests: int = 10, burst_limit: int = 50, window_seconds: int = 1):
        self.max_requests = max_requests
        self.burst_limit = burst_limit
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Acquire a rate limit token"""
        async with self.lock:
            now = time.time()
            
            # Remove old requests
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            
            # Check if we've hit the burst limit
            if len(self.requests) >= self.burst_limit:
                wait_time = self.requests[0] + self.window_seconds - now
                if wait_time > 0:
                    logger.warning(f"Rate limit burst exceeded, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                self.requests = self.requests[1:]
            
            # Check if we need to wait for the rolling window
            if len(self.requests) >= self.max_requests:
                wait_time = self.requests[0] + self.window_seconds - now
                if wait_time > 0:
                    logger.warning(f"Rate limit exceeded, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                self.requests = self.requests[1:]
            
            # Add current request
            self.requests.append(now)
            
    async def release(self):
        """Release a rate limit token (no-op since we use time-based windowing)"""
        pass

class WalletBud(commands.Bot):
    """WalletBud Discord bot"""
    def __init__(self):
        """Initialize the bot"""
        super().__init__(
            command_prefix='!',
            intents=discord.Intents.all(),
            help_command=None
        )
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests=10,  # 10 requests per second
            burst_limit=50,   # 50 requests burst
            window_seconds=1  # 1 second window
        )
        
        # Initialize Blockfrost client
        self.blockfrost_client = None
        
        # Initialize monitoring state
        self.monitoring_paused = False
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
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
        """Make a rate-limited request to the Blockfrost API
        
        Args:
            func: The Blockfrost API function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            The result of the API call
            
        Raises:
            Exception: If the API call fails
        """
        await self.rate_limiter.acquire()
        try:
            loop = asyncio.get_event_loop()
            if args and not kwargs:
                # If only positional args, pass them directly
                return await loop.run_in_executor(None, func, *args)
            elif kwargs and not args:
                # If only keyword args, pass them as kwargs
                return await loop.run_in_executor(None, lambda: func(**kwargs))
            else:
                # If both, combine them
                return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
        finally:
            await self.rate_limiter.release()

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

                # Get current wallet state
                current_balance = 0
                current_tokens = {}
                try:
                    current_balance, current_tokens = await self.rate_limited_request(
                        self.blockfrost_client.address_total,
                        address
                    )
                except Exception as e:
                    logger.error(f"Error getting wallet state: {str(e)}")
                    return

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
                wallet = await get_wallet(str(user_id), address)
                if not wallet:
                    logger.error(f"Could not find wallet {address} for user {user_id}")
                    return
                    
                wallet_id = wallet['wallet_id']

                # Update last checked timestamp
                await update_last_checked(wallet_id)
                
                # Get current UTXOs
                current_utxos = await self.rate_limited_request(
                    self.blockfrost_client.address_utxos,
                    address
                )
                
                # Convert UTXOs to comparable format
                current_state = {
                    utxo.tx_hash: {
                        amount.unit: amount.quantity 
                        for amount in utxo.amount
                    }
                    for utxo in current_utxos
                }
                
                # Get previous state
                prev_state_json = await get_utxo_state(address)
                prev_state = json.loads(prev_state_json) if prev_state_json else {}
                
                # Store current state
                await store_utxo_state(address, json.dumps(current_state))
                
                # Check YUMMI balance
                has_enough, balance = await self.verify_yummi_balance(address)
                if not has_enough:
                    logger.warning(f"Wallet {address} YUMMI balance too low: {balance:,}")
                    # TODO: Notify user and remove wallet
                    return
                
                # 1. Check ADA Transactions and Unusual Activity
                txs = await self.rate_limited_request(
                    self.blockfrost_client.address_transactions,
                    address,
                    count=10,
                    page=1,
                    order='desc'
                )
                
                # Check for unusual transaction frequency
                recent_txs = await get_recent_transactions(address, hours=1)
                if len(recent_txs) > MAX_TX_PER_HOUR:
                    embed = discord.Embed(
                        title="⚠️ Unusual Wallet Activity",
                        description="High frequency of transactions detected!",
                        color=discord.Color.orange()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{address}`",
                        inline=False
                    )
                    embed.add_field(
                        name="Transactions (Last Hour)",
                        value=f"`{len(recent_txs)}`",
                        inline=True
                    )
                    embed.add_field(
                        name="Threshold",
                        value=f"`{MAX_TX_PER_HOUR}`",
                        inline=True
                    )
                    if await self.should_notify(int(user_id), "unusual_activity"):
                        await user.send(embed=embed)
                
                for tx in txs:
                    try:
                        tx_hash = tx.tx_hash if hasattr(tx, 'tx_hash') else None
                        if not tx_hash:
                            continue
                            
                        # Check if transaction is new
                        is_new = await add_transaction(address, tx_hash)
                        if is_new:
                            # Get transaction details to check ADA movement
                            tx_details = await self.rate_limited_request(
                                self.blockfrost_client.transaction_utxos,
                                tx_hash
                            )
                            
                            # Calculate ADA change
                            ada_change = 0
                            for output in tx_details.outputs:
                                if output.address == address:
                                    for amount in output.amount:
                                        if amount.unit == "lovelace":
                                            ada_change += int(amount.quantity)
                            
                            for input in tx_details.inputs:
                                if input.address == address:
                                    for amount in input.amount:
                                        if amount.unit == "lovelace":
                                            ada_change -= int(amount.quantity)
                            
                            if ada_change != 0:
                                # Send ADA transaction notification
                                embed = discord.Embed(
                                    title="💰 ADA Transaction Detected",
                                    description=f"{'Received' if ada_change > 0 else 'Sent'} ADA in your monitored wallet.",
                                    color=discord.Color.green() if ada_change > 0 else discord.Color.red()
                                )
                                embed.add_field(
                                    name="Wallet",
                                    value=f"`{address}`",
                                    inline=False
                                )
                                embed.add_field(
                                    name="Amount",
                                    value=f"`{ada_change / 1_000_000:,.6f} ADA`",
                                    inline=False
                                )
                                embed.add_field(
                                    name="Transaction Hash",
                                    value=f"`{tx_hash}`",
                                    inline=False
                                )
                                if await self.should_notify(int(user_id), "ada_transactions"):
                                    await user.send(embed=embed)
                    
                    except Exception as e:
                        logger.error(f"Error processing transaction {tx_hash}: {str(e)}")
                        continue
                
                # 2. Check for New Tokens (excluding NFTs)
                current_tokens = set()
                current_nfts = set()
                total_ada = 0
                
                for utxo in current_utxos:
                    for amount in utxo.amount:
                        if amount.unit == "lovelace":
                            total_ada += int(amount.quantity)
                        elif amount.unit != "lovelace":
                            # Check if it's an NFT
                            try:
                                asset_details = await self.rate_limited_request(
                                    self.blockfrost_client.asset,
                                    amount.unit
                                )
                                if hasattr(asset_details, 'onchain_metadata'):
                                    current_nfts.add(amount.unit)
                                else:
                                    current_tokens.add(amount.unit)
                            except Exception:
                                # If we can't get metadata, treat as regular token
                                current_tokens.add(amount.unit)
                
                prev_tokens = set()
                prev_nfts = set()
                for utxo_data in prev_state.values():
                    for unit in utxo_data.keys():
                        if unit != "lovelace":
                            if unit in current_nfts:
                                prev_nfts.add(unit)
                            else:
                                prev_tokens.add(unit)
                
                # Check for new tokens
                new_tokens = await get_new_tokens(address, prev_tokens, current_tokens)
                if new_tokens:
                    for token in new_tokens:
                        try:
                            # Skip if already processed
                            curr_bal = current_state.get(token, 0)
                            prev_bal = prev_state.get(token, 0)
                            
                            if await is_token_change_processed(address, token, prev_bal, curr_bal):
                                continue
                                
                            # Get token details
                            asset_details = await self.rate_limited_request(
                                self.blockfrost_client.asset,
                                token
                            )
                            
                            # Get token name and metadata
                            token_name = "Unknown Token"
                            if hasattr(asset_details, 'onchain_metadata') and asset_details.onchain_metadata:
                                token_name = asset_details.onchain_metadata.get('name', 'Unknown Token')
                            elif hasattr(asset_details, 'metadata') and asset_details.metadata:
                                token_name = asset_details.metadata.get('name', 'Unknown Token')
                            
                            # Create notification
                            embed = discord.Embed(
                                title="🪙 Token Balance Change",
                                description=f"Your {token_name} balance has changed.",
                                color=discord.Color.blue()
                            )
                            embed.add_field(
                                name="Wallet",
                                value=f"`{address}`",
                                inline=False
                            )
                            embed.add_field(
                                name="Token",
                                value=f"`{token_name}`",
                                inline=True
                            )
                            embed.add_field(
                                name="Previous Balance",
                                value=f"`{prev_bal:,}`",
                                inline=True
                            )
                            embed.add_field(
                                name="New Balance",
                                value=f"`{curr_bal:,}`",
                                inline=True
                            )
                            embed.add_field(
                                name="Change",
                                value=f"`{curr_bal - prev_bal:+,}`",
                                inline=True
                            )
                            
                            # Record this change
                            await add_processed_token_change(address, token, prev_bal, curr_bal)
                            
                            if await self.should_notify(int(user_id), "token_changes"):
                                await user.send(embed=embed)
                                
                        except Exception as e:
                            logger.error(f"Error processing token change for {token}: {str(e)}")
                            continue
                
                # Check for removed NFTs
                removed_nfts = await get_removed_nfts(address, prev_nfts, current_nfts)
                if removed_nfts:
                    embed = discord.Embed(
                        title="🎨 NFT Removed",
                        description="NFT(s) have been removed from your wallet.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{address}`",
                        inline=False
                    )
                    for nft in removed_nfts:
                        try:
                            asset_details = await self.rate_limited_request(
                                self.blockfrost_client.asset,
                                nft
                            )
                            name = asset_details.onchain_metadata.get('name', 'Unknown NFT')
                            embed.add_field(
                                name=name,
                                value=f"Policy: `{nft[:56]}`",
                                inline=False
                            )
                        except Exception:
                            continue
                    if await self.should_notify(int(user_id), "nft_updates"):
                        await user.send(embed=embed)
                
                # Check for low ADA balance
                is_low, balance_ada = await check_ada_balance(address, total_ada)
                if is_low:
                    embed = discord.Embed(
                        title="⚠️ Low ADA Balance",
                        description="Your wallet's ADA balance is below the minimum threshold!",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{address}`",
                        inline=False
                    )
                    embed.add_field(
                        name="Current Balance",
                        value=f"`{balance_ada:,.6f} ADA`",
                        inline=True
                    )
                    embed.add_field(
                        name="Minimum Required",
                        value=f"`{MIN_ADA_BALANCE:,.6f} ADA`",
                        inline=True
                    )
                    if await self.should_notify(int(user_id), "low_balance"):
                        await user.send(embed=embed)
                
                # Continue with other checks (staking rewards, stake key changes)
                await self._check_staking_and_stake_key(address, user)
                
        except Exception as e:
            logger.error(f"Error checking wallet {address}: {str(e)}")
            
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
                # Default to True if no settings exist
                return True
            return settings.get(notification_type, True)
            
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            # Default to True on error
            return True

    async def _check_staking_and_stake_key(self, address: str, user: discord.User):
        """Helper method to check staking rewards and stake key changes"""
        try:
            # Check Staking Rewards
            rewards = await self.rate_limited_request(
                self.blockfrost_client.account_rewards,
                address,
                count=5,
                page=1
            )
            
            for reward in rewards:
                if not await is_reward_processed(address, reward.epoch):
                    await add_processed_reward(address, reward.epoch)
                    
                    embed = discord.Embed(
                        title="🎁 Staking Reward Received",
                        description="You've received staking rewards!",
                        color=discord.Color.gold()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{address}`",
                        inline=False
                    )
                    embed.add_field(
                        name="Amount",
                        value=f"`{int(reward.amount) / 1_000_000:,.6f} ADA`",
                        inline=True
                    )
                    embed.add_field(
                        name="Epoch",
                        value=f"`{reward.epoch}`",
                        inline=True
                    )
                    if await self.should_notify(int(user.id), "staking_rewards"):
                        await user.send(embed=embed)
            
            # Check Stake Key Changes
            stake_address = await self.rate_limited_request(
                self.blockfrost_client.address_details,
                address
            )
            
            if stake_address and stake_address.stake_address:
                current_stake = stake_address.stake_address
                prev_stake = await get_stake_address(address)
                
                if current_stake != prev_stake:
                    await update_stake_address(address, current_stake)
                    
                    embed = discord.Embed(
                        title="🔑 Stake Key Change Detected",
                        description="Your stake key registration has changed.",
                        color=discord.Color.yellow()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{address}`",
                        inline=False
                    )
                    embed.add_field(
                        name="New Stake Address",
                        value=f"`{current_stake}`",
                        inline=False
                    )
                    if await self.should_notify(int(user.id), "stake_changes"):
                        await user.send(embed=embed)
                    
        except Exception as e:
            logger.error(f"Error checking staking and stake key: {str(e)}")

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

            @self.tree.command(name="toggle", description="Toggle a notification type")
            @app_commands.describe(notification_type="Type of notification to toggle")
            @app_commands.choices(notification_type=[
                app_commands.Choice(name="ADA Transactions", value="ada"),
                app_commands.Choice(name="Token Changes", value="token"),
                app_commands.Choice(name="NFT Updates", value="nft"),
                app_commands.Choice(name="Staking Rewards", value="staking"),
                app_commands.Choice(name="Stake Key Changes", value="stake"),
                app_commands.Choice(name="Low Balance Alerts", value="balance")
            ])
            async def toggle(interaction: discord.Interaction, notification_type: str):
                await self._toggle_notification(interaction, notification_type)

            # Sync the commands
            await self.tree.sync()
            logger.info("Commands synced successfully")

        except Exception as e:
            logger.error(f"Failed to set up commands: {str(e)}")
            raise

    async def _add_wallet(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            # Check if wallet is already registered
            existing_wallet = await get_wallet_for_user(str(interaction.user.id), address)
            if existing_wallet:
                await interaction.response.send_message("❌ Wallet already registered!", ephemeral=True)
                return
            
            # Add wallet to database
            await add_wallet(str(interaction.user.id), address)
            
            # Update last checked timestamp
            await update_last_checked(address)
            
            await interaction.response.send_message("✅ Wallet added successfully!", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while adding your wallet.", ephemeral=True)

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet is registered
            if not await get_wallet(address):
                await interaction.response.send_message("❌ Wallet not registered!", ephemeral=True)
                return
            
            # Remove wallet from database
            await remove_wallet(address)
            
            await interaction.response.send_message("✅ Wallet removed successfully!", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while removing your wallet.", ephemeral=True)

    async def _list_wallets(self, interaction: discord.Interaction):
        """List all registered wallets"""
        try:
            # Get all wallets for user
            wallets = await get_all_wallets()
            user_wallets = [w for w in wallets if w['user_id'] == str(interaction.user.id)]
            
            if not user_wallets:
                await interaction.response.send_message("❌ No wallets registered!", ephemeral=True)
                return
            
            # Create embed
            embed = discord.Embed(
                title="📝 Registered Wallets",
                description="Your registered wallets:",
                color=discord.Color.blue()
            )
            
            # Add each wallet
            for wallet in user_wallets:
                embed.add_field(
                    name="Wallet",
                    value=f"`{wallet['address']}`",
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while listing your wallets.", ephemeral=True)

    async def _help(self, interaction: discord.Interaction):
        """Show bot help and commands"""
        try:
            # Create embed
            embed = discord.Embed(
                title="🤔 Help and Commands",
                description="Available commands:",
                color=discord.Color.blue()
            )
            
            # Add each command
            for command in self.tree.get_commands():
                embed.add_field(
                    name=f"/{command.name}",
                    value=command.description,
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing help: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while showing help.", ephemeral=True)

    async def _health(self, interaction: discord.Interaction):
        """Check bot and API status"""
        try:
            # Check Blockfrost API status
            blockfrost_status = await self.rate_limited_request(
                self.blockfrost_client.health
            )
            
            # Create embed
            embed = discord.Embed(
                title="🏥 Bot and API Status",
                description="Current status:",
                color=discord.Color.blue()
            )
            
            # Add Blockfrost API status
            embed.add_field(
                name="Blockfrost API",
                value=f"`{blockfrost_status.is_healthy}`",
                inline=False
            )
            
            # Add bot status
            embed.add_field(
                name="Bot",
                value="✅ Online",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error checking health: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while checking status.", ephemeral=True)

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
                    balance_info = await self.rate_limited_request(
                        self.blockfrost_client.address_total,
                        address
                    )
                    
                    if balance_info:
                        # Calculate ADA balance
                        ada_balance = int(balance_info.received_sum[0].quantity) / 1_000_000
                        total_ada += ada_balance
                        
                        # Add wallet info
                        embed.add_field(
                            name=f"Wallet `{address[:20]}...`",
                            value=f"Balance: `{ada_balance:,.6f} ADA`\nTransactions: `{balance_info.tx_count}`",
                            inline=False
                        )
                        
                except Exception as e:
                    logger.error(f"Error getting balance for {address}: {str(e)}")
                    embed.add_field(
                        name=f"Wallet `{address[:20]}...`",
                        value="❌ Failed to get balance",
                        inline=False
                    )
            
            # Add total balance
            embed.add_field(
                name="Total ADA Balance",
                value=f"`{total_ada:,.6f} ADA`",
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
                await interaction.response.send_message("❌ You don't have any notification settings! Use `/addwallet` first.", ephemeral=True)
                return
                        
            # Create embed
            embed = discord.Embed(
                title="🔔 Notification Settings",
                description="Your current notification settings:",
                color=discord.Color.blue()
            )
            
            # Add each setting
            settings_display = {
                "ada_transactions": "ADA Transactions",
                "token_changes": "Token Changes",
                "nft_updates": "NFT Updates",
                "staking_rewards": "Staking Rewards",
                "stake_changes": "Stake Key Changes",
                "low_balance": "Low Balance Alerts"
            }
            
            for setting, display_name in settings_display.items():
                status = settings.get(setting, False)
                embed.add_field(
                    name=display_name,
                    value=f"{'✅ Enabled' if status else '❌ Disabled'}",
                    inline=True
                )
            
            # Add instructions
            embed.add_field(
                name="How to Change",
                value="Use `/toggle_notification [type]` to enable/disable notifications",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
                    
        except Exception as e:
            logger.error(f"Error getting notification settings: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while getting your notification settings.", ephemeral=True)

    async def _toggle_notification(self, interaction: discord.Interaction, notification_type: str):
        """Toggle a specific notification type on/off"""
        try:
            valid_types = {
                "ada": "ada_transactions",
                "token": "token_changes",
                "nft": "nft_updates",
                "staking": "staking_rewards",
                "stake": "stake_changes",
                "balance": "low_balance"
            }
            
            if notification_type not in valid_types:
                type_list = ", ".join(f"`{t}`" for t in valid_types.keys())
                await interaction.response.send_message(f"❌ Invalid notification type! Valid types are: {type_list}", ephemeral=True)
                return
                        
            setting_key = valid_types[notification_type]
            settings = await get_notification_settings(str(interaction.user.id))
            
            if not settings:
                # Initialize settings if they don't exist
                default_settings = {
                    "ada_transactions": True,
                    "token_changes": True,
                    "nft_updates": True,
                    "staking_rewards": True,
                    "stake_changes": True,
                    "low_balance": True
                }
                await update_notification_setting(str(interaction.user.id), setting_key, True)
                settings = default_settings
            
            # Toggle the setting
            new_status = not settings.get(setting_key, True)
            success = await update_notification_setting(str(interaction.user.id), setting_key, new_status)
            
            if success:
                status = "enabled" if new_status else "disabled"
                await interaction.response.send_message(f"✅ Successfully {status} {notification_type} notifications!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to update notification setting.", ephemeral=True)
                    
        except Exception as e:
            logger.error(f"Error toggling notification: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while updating your notification settings.", ephemeral=True)

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        
        # Constants
        REQUIRED_YUMMI_TOKENS = 25000
        WALLET_CHECK_INTERVAL = 60  # seconds
        MAX_TX_PER_HOUR = 10
        MIN_ADA_BALANCE = 2  # ADA
        YUMMI_POLICY_ID = "f9f5af5a6c9df7c7f9c2f86c5e1f2a7c"
        
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
