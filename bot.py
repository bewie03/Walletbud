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
    get_all_wallets_for_user,
    get_wallet_id,
    get_last_yummi_check,
    update_last_yummi_check,
    get_yummi_warning_count,
    increment_yummi_warning
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

    async def check_yummi_requirement(self, address: str, user_id: str = None):
        """Check if wallet meets YUMMI token requirement
        
        Args:
            address (str): Wallet address to check
            user_id (str, optional): User ID for notifications. Defaults to None.
            
        Returns:
            tuple: (bool, int) - (meets requirement, current balance)
        """
        try:
            # Get UTXOs
            utxos = await self.rate_limited_request(
                self.blockfrost_client.address_utxos,
                address
            )
            
            # Calculate YUMMI balance
            yummi_balance = sum(
                int(amount.quantity)
                for utxo in utxos
                for amount in utxo.amount
                if amount.unit == '29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494d4d49'
            )
            
            meets_requirement = yummi_balance >= 25000
            
            # If user_id provided and requirement not met, notify user
            if user_id and not meets_requirement:
                try:
                    user = await self.fetch_user(int(user_id))
                    if user:
                        await user.send(
                            f"⚠️ **YUMMI Requirement Not Met!**\n"
                            f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                            f"Current YUMMI: `{yummi_balance:,}`\n"
                            f"Required: `25,000`\n\n"
                            f"Your wallet will be removed from monitoring if the requirement is not met within 24 hours."
                        )
                except Exception as e:
                    logger.error(f"Error notifying user about YUMMI requirement: {str(e)}")
            
            return meets_requirement, yummi_balance
            
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
                    meets_req, balance = await self.check_yummi_requirement(address, user_id)
                    await update_last_yummi_check(address)
                    
                    if not meets_req:
                        # Get current warning count
                        warning_count = await get_yummi_warning_count(address)
                        
                        if warning_count >= 4:  # Remove wallet after 24 hours (4 warnings)
                            await remove_wallet(user_id, address)
                            try:
                                user = await self.fetch_user(int(user_id))
                                if user:
                                    await user.send(
                                        f"❌ **Wallet Removed**\n"
                                        f"Wallet: `{address[:8]}...{address[-8:]}`\n"
                                        f"Reason: YUMMI requirement not met for 24 hours\n"
                                        f"Current YUMMI: `{balance:,}`\n"
                                        f"Required: `25,000`"
                                    )
                            except Exception as e:
                                logger.error(f"Error notifying user about wallet removal: {str(e)}")
                            return
                        else:
                            await increment_yummi_warning(address)
                
                # Get current wallet state
                try:
                    # Get UTXOs
                    utxos = await self.rate_limited_request(
                        self.blockfrost_client.address_utxos,
                        address
                    )
                    
                    # Calculate current balance
                    current_balance = sum(
                        int(utxo.amount[0].quantity) / 1_000_000 
                        for utxo in utxos 
                        if utxo.amount and utxo.amount[0].unit == 'lovelace'
                    )
                    
                    # Calculate current token amounts
                    current_tokens = {}
                    for utxo in utxos:
                        for amount in utxo.amount:
                            if amount.unit != 'lovelace':
                                current_tokens[amount.unit] = current_tokens.get(amount.unit, 0) + int(amount.quantity)
                
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
                wallet_id = await get_wallet_id(str(user_id), address)
                if not wallet_id:
                    logger.error(f"Could not find wallet ID for {address}")
                    return

                # Update last checked timestamp
                await update_last_checked(wallet_id)

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

        except Exception as e:
            logger.error(f"Error checking wallet: {str(e)}")

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
            # Validate address format
            if not address.startswith('addr1'):
                await interaction.response.send_message("❌ Invalid wallet address! Address must start with 'addr1'.", ephemeral=True)
                return
                
            # Check if wallet exists and get UTXOs
            try:
                utxos = await self.rate_limited_request(
                    self.blockfrost_client.address_utxos,
                    address
                )
                if not utxos:
                    await interaction.response.send_message("❌ Invalid wallet address! Could not find wallet on blockchain.", ephemeral=True)
                    return

                # Check YUMMI token requirement (25,000 minimum)
                yummi_amount = 0
                for utxo in utxos:
                    for amount in utxo.amount:
                        if amount.unit == '29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494d4d49':  # YUMMI
                            yummi_amount += int(amount.quantity)

                if yummi_amount < 25000:
                    await interaction.response.send_message("❌ Wallet must hold at least 25,000 YUMMI tokens!", ephemeral=True)
                    return

            except Exception as e:
                logger.error(f"Error validating address: {str(e)}")
                await interaction.response.send_message("❌ Invalid wallet address! Could not verify wallet on blockchain.", ephemeral=True)
                return
            
            # Check if wallet is already registered
            if await get_wallet(str(interaction.user.id), address):
                await interaction.response.send_message("❌ This wallet is already registered!", ephemeral=True)
                return
            
            # Add wallet to database
            success = await add_wallet(str(interaction.user.id), address)
            if success:
                await interaction.response.send_message("✅ Wallet added successfully!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to add wallet.", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while adding your wallet.", ephemeral=True)

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists
            wallet = await get_wallet(str(interaction.user.id), address)
            if not wallet:
                await interaction.response.send_message("❌ Wallet not found!", ephemeral=True)
                return
            
            # Remove wallet
            success = await remove_wallet(str(interaction.user.id), address)
            if success:
                await interaction.response.send_message("✅ Wallet removed successfully!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to remove wallet.", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await interaction.response.send_message("❌ An error occurred while removing the wallet.", ephemeral=True)

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
                    utxos = await self.rate_limited_request(
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
                            if amount.unit == '29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494d4d49'
                        )
                        
                        # Get transaction count
                        tx_info = await self.rate_limited_request(
                            self.blockfrost_client.address_total,
                            address
                        )
                        tx_count = tx_info.tx_count if tx_info else 0
                        
                        # Format address for display
                        short_address = f"{address[:8]}...{address[-8:]}"
                        
                        # Add wallet field
                        embed.add_field(
                            name=f"🏦 Wallet #{i}",
                            value=(
                                f"**Address:** `{short_address}`\n"
                                f"**Balance:** `{ada_balance:,.2f} ADA`\n"
                                f"**YUMMI:** `{yummi_balance:,}`\n"
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
                description="Monitor your Cardano wallets and receive notifications about important events!",
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
                    "• `balance` - ADA balance changes\n"
                    "• `tokens` - Token transfers\n"
                    "• `nfts` - NFT transfers\n"
                    "• `staking` - Staking rewards\n"
                    "• `stake_key` - Stake key changes"
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
                    utxos = await self.rate_limited_request(
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
                        
                        # Get transaction count
                        tx_info = await self.rate_limited_request(
                            self.blockfrost_client.address_total,
                            address
                        )
                        tx_count = tx_info.tx_count if tx_info else 0
                        
                        # Add wallet info
                        embed.add_field(
                            name=f"Wallet `{address[:20]}...`",
                            value=f"Balance: `{ada_balance:,.6f} ADA`\nTransactions: `{tx_count}`",
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
