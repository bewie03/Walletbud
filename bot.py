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
    add_processed_token_change
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
            
            # Setup commands
            await self.setup_commands()
            logger.info("Commands setup complete")
            
            # Start wallet monitoring task
            self.bg_task = self.loop.create_task(self.check_wallets())
            logger.info("Started wallet monitoring task")
            
            logger.info("Bot initialization complete!")
            
        except Exception as e:
            logger.error(f"Error in setup: {str(e)}")
            raise

    async def setup_commands(self):
        """Set up bot commands using app_commands"""
        try:
            logger.info("Setting up commands...")
            
            # Add commands using app_commands.CommandTree
            @self.tree.command(name='addwallet', description='Add a wallet to monitor')
            @dm_only()
            @has_blockfrost()
            async def addwallet(interaction: discord.Interaction, address: str):
                try:
                    # Defer response FIRST before any processing
                    await interaction.response.defer(ephemeral=True)
                    logger.info(f"Adding wallet {address} for user {interaction.user.id}")
                    
                    # Basic address validation
                    if not address or len(address) < 10 or not address.startswith('addr1'):
                        await interaction.followup.send(
                            "❌ Invalid Cardano wallet address format. Please check and try again.",
                            ephemeral=True
                        )
                        return

                    # Verify address with Blockfrost
                    try:
                        logger.info(f"Verifying address {address} exists...")
                        await self.rate_limited_request(
                            self.blockfrost_client.address,
                            address
                        )
                        
                        # Check YUMMI token balance
                        has_enough, balance = await self.verify_yummi_balance(address)
                        if not has_enough:
                            await interaction.followup.send(
                                f"❌ This wallet does not have the required {REQUIRED_YUMMI_TOKENS:,} YUMMI tokens. Current balance: {balance:,}",
                                ephemeral=True
                            )
                            return
                            
                        # Add wallet to database
                        try:
                            await add_wallet(str(interaction.user.id), address)
                            await interaction.followup.send(
                                "✅ Successfully added wallet to monitoring!",
                                ephemeral=True
                            )
                        except Exception as e:
                            logger.error(f"Failed to add wallet to database: {str(e)}")
                            await interaction.followup.send(
                                "❌ Failed to add wallet. Please try again later.",
                                ephemeral=True
                            )
                            
                    except Exception as e:
                        logger.error(f"Failed to verify address {address}: {str(e)}")
                        await interaction.followup.send(
                            "❌ Invalid wallet address or API error. Please check the address and try again.",
                            ephemeral=True
                        )
                        
                except Exception as e:
                    logger.error(f"Error in addwallet command: {str(e)}")
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            "❌ An error occurred. Please try again later.",
                            ephemeral=True
                        )
                    else:
                        await interaction.followup.send(
                            "❌ An error occurred. Please try again later.",
                            ephemeral=True
                        )

            @self.tree.command(name='removewallet', description='Remove a wallet from monitoring')
            @dm_only()
            async def removewallet(interaction: discord.Interaction, address: str):
                try:
                    # Defer response FIRST before any processing
                    await interaction.response.defer(ephemeral=True)
                    logger.info(f"Removing wallet {address} for user {interaction.user.id}")
                    
                    # Remove from database
                    success = await remove_wallet(str(interaction.user.id), address)
                    if success:
                        await interaction.followup.send(
                            f"✅ Successfully removed wallet `{address}` from monitoring.",
                            ephemeral=True
                        )
                    else:
                        await interaction.followup.send(
                            f"❌ Wallet `{address}` was not found in your monitored wallets.",
                            ephemeral=True
                        )
                        
                except Exception as e:
                    logger.error(f"Error in removewallet command: {str(e)}")
                    await interaction.followup.send(
                        "❌ An error occurred. Please try again later.",
                        ephemeral=True
                    )
            
            @self.tree.command(name='listwallets', description='List your monitored wallets')
            @dm_only()
            async def listwallets(interaction: discord.Interaction):
                try:
                    # Defer response FIRST before any processing
                    await interaction.response.defer(ephemeral=True)
                    logger.info(f"Listing wallets for user {interaction.user.id}")
                    
                    # Get wallets
                    all_wallets = await get_all_wallets()
                    wallets = [w['address'] for w in all_wallets if w['user_id'] == str(interaction.user.id)]
                    
                    if not wallets:
                        await interaction.followup.send(
                            "You don't have any wallets being monitored.",
                            ephemeral=True
                        )
                        return
                    
                    # Create embed
                    embed = discord.Embed(
                        title="🔍 Your Monitored Wallets",
                        color=discord.Color.blue()
                    )
                    
                    for wallet in wallets:
                        embed.add_field(
                            name="Wallet Address",
                            value=f"`{wallet}`",
                            inline=False
                        )
                    
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    
                except Exception as e:
                    logger.error(f"Error in listwallets command: {str(e)}")
                    await interaction.followup.send(
                        "❌ An error occurred. Please try again later.",
                        ephemeral=True
                    )

            @self.tree.command(name="help", description="Show help information")
            async def help(interaction: discord.Interaction):
                try:
                    await self.process_interaction(interaction, ephemeral=True)
                    
                    embed = discord.Embed(
                        title="📚 WalletBud Help",
                        description="Monitor your Cardano wallets for YUMMI tokens and ADA balance",
                        color=discord.Color.blue()
                    )
                    
                    embed.add_field(
                        name="/addwallet <address>",
                        value="Add a wallet to monitor (DM only)",
                        inline=False
                    )
                    embed.add_field(
                        name="/removewallet <address>",
                        value="Remove a wallet from monitoring (DM only)",
                        inline=False
                    )
                    embed.add_field(
                        name="/listwallets",
                        value="List your monitored wallets (DM only)",
                        inline=False
                    )
                    embed.add_field(
                        name="/balance <address>",
                        value="Check total ADA balance in a wallet",
                        inline=False
                    )
                    embed.add_field(
                        name="/health",
                        value="Check bot and API status",
                        inline=False
                    )
                    
                    embed.set_footer(text="For support, please contact the bot owner")
                    
                    await interaction.followup.send(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Error in help command: {str(e)}")
                    await interaction.followup.send(
                        "❌ Failed to show help. Please try again later.",
                        ephemeral=True
                    )

            @self.tree.command(name="balance", description="Check total ADA balance in a wallet")
            @has_blockfrost
            async def balance(interaction: discord.Interaction, address: str):
                try:
                    await self.process_interaction(interaction, ephemeral=True)
                    
                    # Get total balance using address/total endpoint
                    balance_info = await self.rate_limited_request(
                        self.blockfrost_client.address_total,
                        address
                    )
                    
                    if not balance_info or not balance_info.received_sum:
                        await interaction.followup.send("❌ Could not fetch wallet balance.")
                        return
                    
                    # Find lovelace amount (native ADA)
                    lovelace_amount = 0
                    for token in balance_info.received_sum:
                        if token.unit == "lovelace":
                            lovelace_amount = int(token.quantity)
                            break
                    
                    # Convert lovelace to ADA (1 ADA = 1,000,000 lovelace)
                    total_ada = lovelace_amount / 1_000_000
                    
                    embed = discord.Embed(
                        title="💰 Wallet Balance",
                        description=f"Address: `{address}`",
                        color=discord.Color.green()
                    )
                    
                    embed.add_field(
                        name="Total ADA",
                        value=f"₳ {total_ada:,.2f}",
                        inline=False
                    )
                    
                    await interaction.followup.send(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Error checking balance for {address}: {str(e)}")
                    await interaction.followup.send(
                        "❌ An error occurred while checking the wallet balance.",
                        ephemeral=True
                    )
            
            @self.tree.command(name='health', description='Check bot and API status')
            async def health(interaction: discord.Interaction):
                try:
                    # Check Blockfrost connection
                    blockfrost_status = "✅ Connected" if self.blockfrost_client else "❌ Not Connected"
                    try:
                        if self.blockfrost_client:
                            health = await asyncio.wait_for(
                                asyncio.to_thread(self.blockfrost_client.health),
                                timeout=5.0
                            )
                            if not health:
                                blockfrost_status = "❌ Not Connected"
                    except Exception:
                        blockfrost_status = "❌ Not Connected"
                    
                    # Create embed
                    embed = discord.Embed(
                        title="🔍 Bot Status",
                        color=discord.Color.blue()
                    )
                    
                    # Add fields
                    embed.add_field(
                        name="Bot Status",
                        value="✅ Online",
                        inline=True
                    )
                    embed.add_field(
                        name="Blockfrost API",
                        value=blockfrost_status,
                        inline=True
                    )
                    embed.add_field(
                        name="Monitoring",
                        value="✅ Active" if not self.monitoring_paused else "❌ Paused",
                        inline=True
                    )
                    
                    # Add timestamp
                    embed.timestamp = discord.utils.utcnow()
                    
                    await interaction.response.send_message(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Error in health command: {str(e)}")
                    await interaction.response.send_message(
                        "❌ Failed to get bot status. Please try again later.",
                        ephemeral=True
                    )

            @self.tree.command(name="balance", description="Get your wallet's current balance")
            async def balance_cmd(self, ctx: commands.Context):
                """Get current balance of your registered wallet"""
                try:
                    # Get user's wallet
                    address = await get_wallet_for_user(ctx.author.id)
                    if not address:
                        await ctx.send("❌ You don't have a registered wallet! Use `/register` first.")
                        return
                        
                    # Get current balance
                    ada_balance, token_balances = await get_wallet_balance(address)
                    
                    # Create embed
                    embed = discord.Embed(
                        title="💰 Wallet Balance",
                        description=f"Current balance for wallet `{address}`",
                        color=discord.Color.blue()
                    )
                    
                    # Add ADA balance
                    embed.add_field(
                        name="ADA Balance",
                        value=f"`{ada_balance / 1_000_000:,.6f} ADA`",
                        inline=False
                    )
                    
                    # Add token balances
                    if token_balances:
                        token_list = []
                        for policy_id, amount in token_balances.items():
                            try:
                                asset_details = await self.rate_limited_request(
                                    self.blockfrost_client.asset,
                                    policy_id
                                )
                                name = asset_details.onchain_metadata.get('name', 'Unknown Token')
                                token_list.append(f"{name}: `{amount:,}`")
                            except Exception:
                                token_list.append(f"Policy {policy_id[:8]}...{policy_id[-8:]}: `{amount:,}`")
                        
                        embed.add_field(
                            name="Token Balances",
                            value="\n".join(token_list) if token_list else "No tokens",
                            inline=False
                        )
                    
                    await ctx.send(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Error getting balance: {str(e)}")
                    await ctx.send("❌ An error occurred while getting your balance.")

            @self.tree.command(name="notifications", description="View and manage your notification settings")
            async def notifications_cmd(self, ctx: commands.Context):
                """View and manage notification settings"""
                try:
                    settings = await get_notification_settings(ctx.author.id)
                    if not settings:
                        await ctx.send("❌ You don't have any notification settings! Use `/register` first.")
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
                        "low_balance": "Low Balance Alerts",
                        "unusual_activity": "Unusual Activity"
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
                    
                    await ctx.send(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Error getting notification settings: {str(e)}")
                    await ctx.send("❌ An error occurred while getting your notification settings.")

            @self.tree.command(name="toggle_notification", description="Toggle a specific notification type")
            async def toggle_notification_cmd(
                self,
                ctx: commands.Context,
                notification_type: str = commands.parameter(
                    description="Type of notification to toggle"
                )
            ):
                """Toggle a specific notification type on/off"""
                try:
                    valid_types = {
                        "ada": "ada_transactions",
                        "token": "token_changes",
                        "nft": "nft_updates",
                        "staking": "staking_rewards",
                        "stake": "stake_changes",
                        "balance": "low_balance",
                        "activity": "unusual_activity"
                    }
                    
                    if notification_type not in valid_types:
                        type_list = ", ".join(f"`{t}`" for t in valid_types.keys())
                        await ctx.send(f"❌ Invalid notification type! Valid types are: {type_list}")
                        return
                        
                    setting_key = valid_types[notification_type]
                    settings = await get_notification_settings(ctx.author.id)
                    
                    if not settings:
                        await ctx.send("❌ You don't have any notification settings! Use `/register` first.")
                        return
                        
                    # Toggle the setting
                    new_status = not settings.get(setting_key, True)
                    success = await update_notification_setting(ctx.author.id, setting_key, new_status)
                    
                    if success:
                        status = "enabled" if new_status else "disabled"
                        await ctx.send(f"✅ Successfully {status} {notification_type} notifications!")
                    else:
                        await ctx.send("❌ Failed to update notification setting.")
                    
                except Exception as e:
                    logger.error(f"Error toggling notification: {str(e)}")
                    await ctx.send("❌ An error occurred while updating your notification settings.")

            # Sync commands with Discord
            logger.info("Syncing commands with Discord...")
            await self.tree.sync()
            logger.info("Commands synced successfully")

        except Exception as e:
            logger.error(f"Failed to set up commands: {str(e)}")
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
                # Get user for notifications
                user_id = await get_user_id_for_wallet(address)
                if not user_id:
                    return
                    
                user = await self.fetch_user(int(user_id))
                if not user:
                    return

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
                    if await self.should_notify(user_id, "unusual_activity"):
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
                                if await self.should_notify(user_id, "ada_transactions"):
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
                            
                            if await self.should_notify(user_id, "token_changes"):
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
                    if await self.should_notify(user_id, "nft_updates"):
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
                    if await self.should_notify(user_id, "low_balance"):
                        await user.send(embed=embed)
                
                # Continue with other checks (staking rewards, stake key changes)
                await self._check_staking_and_stake_key(address, user)
                
        except Exception as e:
            logger.error(f"Error checking wallet {address}: {str(e)}")
            
    async def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if we should send a notification to the user
        
        Args:
            user_id (int): Discord user ID
            notification_type (str): Type of notification
            
        Returns:
            bool: Whether to send the notification
        """
        try:
            settings = await get_notification_settings(user_id)
            return settings.get(notification_type, True)  # Default to True if setting doesn't exist
            
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            return True  # Default to sending notification if error occurs

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
                    if await self.should_notify(user.id, "staking_rewards"):
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
                    if await self.should_notify(user.id, "stake_changes"):
                        await user.send(embed=embed)
                    
        except Exception as e:
            logger.error(f"Error checking staking and stake key: {str(e)}")

    async def verify_yummi_balance(self, address: str) -> tuple[bool, int]:
        """Verify YUMMI token balance with robust asset parsing
        
        Args:
            address (str): Wallet address to check
            
        Returns:
            tuple[bool, int]: (has_enough_tokens, current_balance)
        """
        try:
            # First check if address has any UTXOs with YUMMI tokens
            utxos = await self.rate_limited_request(
                self.blockfrost_client.address_utxos,
                address
            )
            
            # Check if any UTXO contains YUMMI token
            asset = f"{YUMMI_POLICY_ID}{'79756d6d69'}"  # 79756d6d69 is hex for "yummi"
            has_yummi = False
            for utxo in utxos:
                for amount in utxo.amount:
                    if amount.unit == asset:
                        has_yummi = True
                        break
                if has_yummi:
                    break
            
            if not has_yummi:
                logger.info(f"No YUMMI tokens found in address {address}")
                return False, 0
                
            # Now get specific asset UTXOs to calculate total
            try:
                asset_utxos = await self.rate_limited_request(
                    self.blockfrost_client.address_utxos_asset,
                    address=address,
                    asset=asset
                )
                
                # Calculate total YUMMI tokens across all UTXOs
                total_tokens = 0
                for utxo in asset_utxos:
                    for amount in utxo.amount:
                        if amount.unit == asset:
                            total_tokens += int(amount.quantity)
                
                logger.info(f"Found {total_tokens} YUMMI tokens for address {address}")
                return total_tokens >= 25000, total_tokens
                
            except Exception as e:
                if hasattr(e, 'status_code') and e.status_code == 404:
                    logger.info(f"No YUMMI tokens found in address {address} (404 response)")
                    return False, 0
                raise
            
        except Exception as e:
            logger.error(f"Failed to verify YUMMI balance: {str(e)}")
            return False, 0

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

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        
        # Constants
        REQUIRED_YUMMI_TOKENS = 25000
        WALLET_CHECK_INTERVAL = 60  # seconds
        
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
