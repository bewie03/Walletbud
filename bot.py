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
    add_transaction
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

def has_blockfrost():
    """Check if Blockfrost client is available"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.client.blockfrost_client:
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

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
        """Called when the bot starts up"""
        try:
            logger.info("Initializing bot...")
            
            # Initialize database
            logger.info("Initializing database...")
            try:
                await init_db()
                logger.info("Database initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database: {str(e)}")
                raise
            
            # Initialize Blockfrost client
            logger.info("Initializing Blockfrost client...")
            try:
                if not await self.init_blockfrost():
                    raise Exception("Failed to initialize Blockfrost client")
                logger.info("Blockfrost client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Blockfrost client: {str(e)}")
                raise
            
            # Setup commands
            logger.info("Setting up commands...")
            try:
                await self.setup_commands()
                logger.info("Commands set up successfully")
            except Exception as e:
                logger.error(f"Failed to set up commands: {str(e)}")
                raise
            
            logger.info("Bot initialization complete!")
            
        except Exception as e:
            logger.error(f"Error in setup_hook: {str(e)}")
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
                            address=address
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
                    wallets = await get_all_wallets(str(interaction.user.id))
                    
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
                        address=address
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
            
            # Check API key
            if not BLOCKFROST_API_KEY:
                logger.error("BLOCKFROST_API_KEY not set")
                return False
                
            # Log first few characters of API key for debugging
            logger.info(f"Using API key starting with: {BLOCKFROST_API_KEY[:8]}...")
            
            # Create client with correct base URL
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_API_KEY,
                base_url="https://cardano-mainnet.blockfrost.io/api/v0"  # Correct mainnet URL
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

    async def rate_limited_request(self, method, **kwargs):
        """Make a rate-limited request to Blockfrost"""
        await self.rate_limiter.acquire()
        return await asyncio.to_thread(method, **kwargs)

    async def check_wallets(self):
        """Background task to check all wallets periodically"""
        await self.wait_until_ready()
        
        while not self.is_closed():
            try:
                # Get all wallets
                wallets = await get_all_wallets()
                if not wallets:
                    logger.debug("No wallets to check")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue
                
                logger.info(f"Checking {len(wallets)} wallets...")
                
                # Process wallets with concurrency limit
                async with asyncio.TaskGroup() as tg:
                    for wallet in wallets:
                        tg.create_task(self.check_wallet(wallet['address']))
                
            except* Exception as e:
                logger.error(f"Error in check_wallets task: {str(e)}")
                
            finally:
                # Always sleep between checks
                await asyncio.sleep(CHECK_INTERVAL)

    async def check_wallet(self, address: str):
        """Check a single wallet's balance and transactions"""
        try:
            async with self.wallet_task_lock:
                # Get current assets
                assets = await self.rate_limited_request(
                    self.blockfrost_client.address_utxos,
                    address=address,
                    count=100,  # Reasonable limit
                    page=1,
                    order='desc'
                )
                
                # Check YUMMI balance
                has_enough, balance = await self.verify_yummi_balance(address)
                if not has_enough:
                    logger.warning(f"Wallet {address} YUMMI balance too low: {balance:,}")
                    # TODO: Notify user and remove wallet
                    return
                
                # Get recent transactions
                txs = await self.rate_limited_request(
                    self.blockfrost_client.address_transactions,
                    address=address,
                    count=10,  # Only get recent transactions
                    page=1,    # First page
                    order='desc'  # Latest first
                )
                
                # Process new transactions
                for tx in txs:
                    try:
                        # Add to database if new
                        await add_transaction(address, tx.hash)
                    except Exception as e:
                        logger.error(f"Error processing transaction {tx.hash}: {str(e)}")
                        continue
                
        except Exception as e:
            logger.error(f"Error checking wallet {address}: {str(e)}")

    async def verify_yummi_balance(self, address: str) -> tuple[bool, int]:
        """Verify YUMMI token balance with robust asset parsing
        
        Args:
            address (str): Wallet address to check
            
        Returns:
            tuple[bool, int]: (has_enough_tokens, current_balance)
        """
        try:
            # Get YUMMI token UTXOs directly using the asset endpoint
            assets = await self.rate_limited_request(
                self.blockfrost_client.address_utxos_asset,
                address=address,
                asset=f"{YUMMI_POLICY_ID}"
            )
            
            # Calculate total YUMMI tokens
            total_tokens = 0
            for utxo in assets:
                for amount in utxo.amount:
                    if amount.unit == f"{YUMMI_POLICY_ID}":
                        total_tokens += int(amount.quantity)
            
            logger.info(f"Found {total_tokens} YUMMI tokens for address {address}")
            return total_tokens >= REQUIRED_YUMMI_TOKENS, total_tokens
            
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
