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
    """Check if command is used in DM"""
    async def predicate(interaction: discord.Interaction):
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(
                "❌ This command can only be used in DMs.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

def has_blockfrost():
    """Check if Blockfrost client is initialized"""
    async def predicate(interaction: discord.Interaction):
        bot = interaction.client
        if not bot.blockfrost_client:
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

class RateLimiter:
    """Rate limiter for API requests"""
    def __init__(self, requests_per_second, burst_limit):
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit
        self.tokens = burst_limit
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Acquire a rate limit token"""
        async with self.lock:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(
                self.burst_limit,
                self.tokens + time_passed * self.requests_per_second
            )
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.requests_per_second
                await asyncio.sleep(wait_time)
                self.tokens = 1
            
            self.tokens -= 1
            self.last_update = now

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
            requests_per_second=MAX_REQUESTS_PER_SECOND,
            burst_limit=BURST_LIMIT
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
            @app_commands.check(dm_only())
            @app_commands.check(has_blockfrost())
            async def addwallet(interaction: discord.Interaction, address: str):
                await self.addwallet_command(interaction, address)
            
            @self.tree.command(name='removewallet', description='Remove a wallet from monitoring')
            @app_commands.check(dm_only())
            async def removewallet(interaction: discord.Interaction, address: str):
                await self.removewallet_command(interaction, address)
            
            @self.tree.command(name='listwallets', description='List your monitored wallets')
            @app_commands.check(dm_only())
            async def listwallets(interaction: discord.Interaction):
                await self.listwallets_command(interaction)
            
            @self.tree.command(name='help', description='Show bot help and commands')
            async def help(interaction: discord.Interaction):
                await self.help_command(interaction)
            
            @self.tree.command(name='health', description='Check bot health status')
            async def health(interaction: discord.Interaction):
                await self.health_command(interaction)
            
            # Sync with Discord
            logger.info("Syncing commands with Discord...")
            try:
                await self.tree.sync()
                logger.info("Commands synced with Discord successfully")
            except Exception as e:
                logger.error(f"Failed to sync commands with Discord: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Error setting up commands: {str(e)}")
            raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            logger.info("Creating Blockfrost client...")
            
            # Check API key
            if not BLOCKFROST_API_KEY:
                logger.error("BLOCKFROST_API_KEY not set")
                return False
            
            # Create client
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_API_KEY,
                base_url="https://cardano-mainnet.blockfrost.io/api"
            )
            
            # Test connection
            try:
                logger.info("Testing Blockfrost connection...")
                # Run health check in a thread to avoid blocking
                loop = asyncio.get_event_loop()
                health = await loop.run_in_executor(None, self.blockfrost_client.health)
                
                if health:
                    logger.info("Blockfrost health check passed")
                    return True
                else:
                    logger.error("Blockfrost health check failed")
                    self.blockfrost_client = None
                    return False
                
            except Exception as e:
                logger.error(f"Failed to test Blockfrost connection: {str(e)}")
                self.blockfrost_client = None
                return False
            
        except Exception as e:
            logger.error(f"Failed to create Blockfrost client: {str(e)}")
            self.blockfrost_client = None
            return False

    async def addwallet_command(self, interaction: discord.Interaction, address: str):
        """Handle the addwallet command"""
        try:
            if not await self.process_interaction(interaction):
                return
                
            # Verify YUMMI balance
            if not await self.verify_yummi_balance(address):
                await self.send_response(
                    interaction,
                    content=f"Insufficient YUMMI tokens. Required: {REQUIRED_YUMMI_TOKENS:,}",
                    ephemeral=True
                )
                return
                
            # Check wallet exists
            balance, txs, _ = await self.check_wallet(address)
            if not balance:
                await self.send_response(
                    interaction,
                    content="Invalid wallet address or API error",
                    ephemeral=True
                )
                return
                
            # Add wallet to database
            success = await add_wallet(address, str(interaction.user.id))
            if success:
                embed = discord.Embed(
                    title="Wallet Added",
                    description=f"Now monitoring wallet `{address}`",
                    color=discord.Color.green()
                )
                embed.add_field(name="Balance", value=f"{balance['amount'][0]['quantity']} ADA", inline=True)
                embed.add_field(name="Transactions", value=str(len(txs)), inline=True)
                
                await self.send_response(interaction, embed=embed, ephemeral=True)
                logger.info(f"Added wallet {address} for user {interaction.user.id}")
            else:
                await self.send_response(
                    interaction,
                    content="Failed to add wallet. Please try again.",
                    ephemeral=True
                )
                
        except Exception as e:
            logger.error(f"Error in addwallet command: {str(e)}")
            await self.send_response(
                interaction,
                content="An error occurred. Please try again later.",
                ephemeral=True
            )

    async def removewallet_command(self, interaction: discord.Interaction, address: str):
        """Handle the removewallet command"""
        try:
            if not await self.process_interaction(interaction):
                return
                
            # Remove wallet
            if await remove_wallet(str(interaction.user.id), address):
                await self.send_response(
                    interaction,
                    content=f"✅ Successfully removed wallet `{address}` from monitoring.",
                    ephemeral=True
                )
            else:
                await self.send_response(
                    interaction,
                    content=f"❌ Failed to remove wallet `{address}`. Please try again later.",
                    ephemeral=True
                )
                
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await self.send_response(
                interaction,
                content="❌ An error occurred while removing the wallet. Please try again later.",
                ephemeral=True
            )

    async def listwallets_command(self, interaction: discord.Interaction):
        """Handle the listwallets command"""
        try:
            if not await self.process_interaction(interaction):
                return
                
            # Get user's wallets
            wallets = await get_all_wallets(str(interaction.user.id))
            
            if not wallets:
                await self.send_response(
                    interaction,
                    content="You have no monitored wallets.",
                    ephemeral=True
                )
                return
                
            # Create embed
            embed = discord.Embed(
                title="Your Monitored Wallets",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Add wallet info
            for wallet in wallets:
                balance, txs, assets = await self.check_wallet(wallet['address'])
                if balance:
                    value = f"Balance: {balance['amount'][0]['quantity']} ADA\n"
                    value += f"Recent Txs: {len(txs)}\n"
                    value += f"Last Checked: <t:{int(datetime.fromisoformat(wallet['last_checked']).timestamp())}:R>"
                    
                    embed.add_field(
                        name=f"Wallet: `{wallet['address']}`",
                        value=value,
                        inline=False
                    )
                    
            await self.send_response(interaction, embed=embed, ephemeral=True)
            logger.info(f"Listed wallets for user {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in listwallets command: {str(e)}")
            await self.send_response(
                interaction,
                content="An error occurred. Please try again later.",
                ephemeral=True
            )

    async def help_command(self, interaction: discord.Interaction):
        """Handle the help command"""
        try:
            if not await self.process_interaction(interaction, ephemeral=True):
                return
            
            # Create help embed
            embed = discord.Embed(
                title="🤖 Wallet Bud Help",
                description="Here are all available commands:",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Add command descriptions
            embed.add_field(
                name="/addwallet <address>",
                value="Add a Cardano wallet to monitor. Requires YUMMI tokens.",
                inline=False
            )
            
            embed.add_field(
                name="/removewallet <address>",
                value="Remove a wallet from monitoring.",
                inline=False
            )
            
            embed.add_field(
                name="/listwallets",
                value="List all your monitored wallets.",
                inline=False
            )
            
            embed.add_field(
                name="/help",
                value="Show this help message.",
                inline=False
            )
            
            embed.add_field(
                name="/health",
                value="Check bot and API status.",
                inline=False
            )
            
            # Add footer with version
            embed.set_footer(text="Wallet Bud v1.0.0")
            
            # Send the help embed
            await self.send_response(interaction, embed=embed, ephemeral=True)
            logger.info(f"Help command used by {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in help command: {str(e)}")
            await self.send_response(
                interaction,
                content="An error occurred while showing help. Please try again later.",
                ephemeral=True
            )

    async def health_command(self, interaction: discord.Interaction):
        """Handle the health command"""
        try:
            if not await self.process_interaction(interaction, ephemeral=True):
                return
            
            # Check Blockfrost API status
            blockfrost_status = "❌ Not Connected"
            if self.blockfrost_client:
                try:
                    health = await self.blockfrost_client.health()
                    if health:
                        blockfrost_status = "✅ Connected"
                except Exception as e:
                    logger.error(f"Failed to check Blockfrost health: {str(e)}")
            
            # Create status embed
            embed = discord.Embed(
                title="🔍 Bot Status",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="Bot Status", value="✅ Online", inline=True)
            embed.add_field(name="Blockfrost API", value=blockfrost_status, inline=True)
            embed.add_field(name="Monitoring", value="✅ Active" if not self.monitoring_paused else "⏸️ Paused", inline=True)
            
            await self.send_response(interaction, embed=embed, ephemeral=True)
            logger.info(f"Health command used by {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in health command: {str(e)}")
            await self.send_response(
                interaction,
                content="An error occurred while checking health. Please try again later.",
                ephemeral=True
            )

    async def rate_limited_request(self, func, *args, **kwargs):
        """Execute a rate-limited API request with exponential backoff"""
        max_retries = API_RETRY_ATTEMPTS
        base_delay = API_RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                # Wait for rate limit token
                await self.rate_limiter.acquire()
                
                # Execute the API call
                result = await func(*args, **kwargs)
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check for rate limit errors
                if "rate limit" in error_msg:
                    if attempt == max_retries - 1:
                        logger.error(f"Rate limit exceeded after {max_retries} attempts")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit, waiting {delay}s before retry")
                    await asyncio.sleep(delay)
                    continue
                    
                # For other errors, log and raise immediately
                logger.error(f"API request failed: {str(e)}")
                raise

    async def check_wallet(self, address):
        """Check wallet balance and transactions"""
        try:
            # Get wallet details
            balance = await self.rate_limited_request(
                self.blockfrost_client.address,
                address=address
            )
            
            # Get latest transactions
            txs = await self.rate_limited_request(
                self.blockfrost_client.address_transactions,
                address=address,
                count=MAX_TX_HISTORY
            )
            
            # Get YUMMI token balance
            assets = await self.rate_limited_request(
                self.blockfrost_client.address_assets,
                address=address
            )
            
            return balance, txs, assets
        except Exception as e:
            logger.error(f"Error checking wallet {address}: {str(e)}")
            return None, None, None

    async def process_transactions(self, user_id, address, txs):
        """Process new transactions for a wallet"""
        try:
            # Get last known transaction
            result = await get_wallet(address, user_id)
            last_tx = result['last_tx_hash'] if result else None
            
            # Check for new transactions
            for tx in txs:
                if last_tx and tx['hash'] == last_tx:
                    break
                    
                # Get transaction details
                tx_details = await self.rate_limited_request(
                    self.blockfrost_client.transaction,
                    hash=tx['hash']
                )
                
                # Store transaction
                await add_transaction(address, tx['hash'], tx_details['output_amount'][0]['quantity'], tx_details['block_height'], tx_details['block_time'])
                
                # Notify user
                try:
                    user = await self.fetch_user(user_id)
                    if user:
                        embed = discord.Embed(
                            title="New Transaction",
                            description=f"New transaction for wallet `{address}`",
                            color=discord.Color.green(),
                            timestamp=datetime.fromtimestamp(tx_details['block_time'])
                        )
                        embed.add_field(name="Amount", value=f"{tx_details['output_amount'][0]['quantity']} ADA", inline=True)
                        embed.add_field(name="Hash", value=tx['hash'], inline=True)
                        
                        dm_channel = await user.create_dm()
                        await dm_channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error notifying user {user_id}: {str(e)}")
            
            # Update last transaction
            if txs:
                await update_last_checked(address, user_id, txs[0]['hash'])
            
        except Exception as e:
            logger.error(f"Error processing transactions: {str(e)}")

    async def verify_yummi_balance(self, address):
        """Verify YUMMI token balance with robust asset parsing"""
        try:
            # Get asset balance with retries
            assets = await self.rate_limited_request(
                self.blockfrost_client.address_assets,
                address=address
            )
            
            # Find YUMMI token with proper parsing
            yummi_balance = 0
            for asset in assets:
                # Verify both policy_id and unit (full asset ID)
                if not isinstance(asset, dict):
                    logger.error(f"Invalid asset format: {asset}")
                    continue
                    
                policy_id = asset.get('policy_id')
                unit = asset.get('unit')
                
                if not policy_id or not unit:
                    logger.error(f"Missing policy_id or unit in asset: {asset}")
                    continue
                
                # Double check both policy_id and full unit
                if (policy_id == YUMMI_POLICY_ID and 
                    unit.startswith(YUMMI_POLICY_ID)):
                    try:
                        quantity = asset.get('quantity', '0')
                        yummi_balance = int(quantity)
                        logger.info(f"Found YUMMI token: {yummi_balance} units")
                        break
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error parsing YUMMI quantity: {e}")
                        continue
            
            has_enough = yummi_balance >= REQUIRED_YUMMI_TOKENS
            logger.info(f"YUMMI balance check: {yummi_balance}/{REQUIRED_YUMMI_TOKENS} -> {has_enough}")
            return has_enough
            
        except Exception as e:
            logger.error(f"Error verifying YUMMI balance: {str(e)}")
            return False

    async def check_wallets(self):
        """Background task to check all wallets with proper concurrency"""
        if self.processing_wallets:
            logger.warning("Wallet check already in progress, skipping")
            return
            
        try:
            async with self.wallet_task_lock:
                self.processing_wallets = True
                
                # Get all active wallets
                wallets = await get_all_wallets()
                
                if not wallets:
                    return
                    
                # Process wallets in chunks to manage rate limits
                chunk_size = 5  # Process 5 wallets at a time
                for i in range(0, len(wallets), chunk_size):
                    chunk = wallets[i:i + chunk_size]
                    tasks = []
                    
                    # Create tasks for each wallet in chunk
                    for wallet in chunk:
                        task = asyncio.create_task(
                            self.process_wallet(wallet['user_id'], wallet['address'])
                        )
                        tasks.append(task)
                    
                    # Wait for chunk to complete
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Rate limit delay between chunks
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    
        except Exception as e:
            logger.error(f"Error in check_wallets task: {str(e)}")
        finally:
            self.processing_wallets = False

    async def process_wallet(self, user_id, address):
        """Process a single wallet with proper error handling"""
        try:
            # Get wallet data
            balance, txs, assets = await self.check_wallet(address)
            if not balance or not txs:
                logger.error(f"Failed to get wallet data for {address}")
                return
                
            # Process new transactions
            await self.process_transactions(user_id, address, txs)
            
            # Verify YUMMI balance
            has_yummi = await self.verify_yummi_balance(address)
            if not has_yummi:
                # Notify user about low YUMMI balance
                try:
                    user = await self.fetch_user(user_id)
                    if user:
                        embed = discord.Embed(
                            title="⚠️ Low YUMMI Balance",
                            description=f"Your wallet `{address}` has insufficient YUMMI tokens.",
                            color=discord.Color.yellow()
                        )
                        embed.add_field(
                            name="Required", 
                            value=f"{REQUIRED_YUMMI_TOKENS:,} YUMMI",
                            inline=True
                        )
                        
                        dm_channel = await user.create_dm()
                        await dm_channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error notifying user about low YUMMI: {str(e)}")
            
            # Update last checked timestamp
            await update_last_checked(address, user_id)
            
        except Exception as e:
            logger.error(f"Error processing wallet {address}: {str(e)}")

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
        """Called when the bot is ready"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            logger.info(f"Discord API version: {discord.__version__}")
            logger.info(f"Connected to {len(self.guilds)} guilds")
            
            # Log guild information
            for guild in self.guilds:
                logger.info(f"Connected to guild: {guild.name} ({guild.id})")
                
            # Sync commands
            await self.tree.sync()
            logger.info("Commands synced with Discord")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")

    @commands.Cog.listener()
    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Handle command errors"""
        error_msg = None
        
        if isinstance(error, app_commands.CommandOnCooldown):
            error_msg = f"This command is on cooldown. Try again in {error.retry_after:.2f} seconds."
        elif isinstance(error, app_commands.CheckFailure):
            # Already handled by dm_only check
            return
        elif isinstance(error, app_commands.CommandInvokeError):
            logger.error(f"Command error: {str(error.original)}")
            error_msg = "An error occurred while processing your command. Please try again later."
        else:
            logger.error(f"Unhandled command error: {str(error)}")
            error_msg = "An unexpected error occurred. Please try again later."
        
        if error_msg:
            try:
                await interaction.response.send_message(error_msg, ephemeral=True)
            except:
                if not interaction.response.is_done():
                    await interaction.followup.send(error_msg, ephemeral=True)

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
