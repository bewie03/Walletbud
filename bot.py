import os
import sys
import json
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from uuid import uuid4
from functools import wraps
import time

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi
import aiohttp

from config import *
from database import init_db, get_db_connection, add_wallet, remove_wallet, update_last_checked

# Create request ID for logging
def get_request_id():
    return str(uuid4())[:8]

# Set up logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up file handler
log_file = os.path.join('logs', 'bot.log')
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Set up console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure root logger
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logger.info(f"Starting bot with log level: {log_level}")

# Centralized error messages
ERROR_MESSAGES = {
    'api_unavailable': "Bot API is not ready. Please try again in a few minutes.",
    'monitoring_paused': "Wallet monitoring is currently unavailable. Please try again later.",
    'invalid_address': "Invalid wallet address. Please check the address and try again.",
    'wallet_not_found': "Wallet not found. Please check the address and try again.",
    'insufficient_tokens': "Insufficient YUMMI tokens. Please ensure you have enough tokens.",
    'db_error': "Database error occurred. Please try again later.",
    'command_error': "An error occurred while processing your command. Please try again later.",
}

def dm_only():
    """Check if command is used in DM"""
    async def predicate(ctx):
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("This command can only be used in DMs for security.")
            return False
        return True
    return commands.check(predicate)

def has_blockfrost():
    """Check if Blockfrost client is initialized"""
    async def predicate(ctx):
        if not ctx.bot.blockfrost_client:
            await ctx.send("Bot API is not ready. Please try again in a few minutes.")
            logger.error("Command failed - Blockfrost client not initialized")
            return False
        return True
    return commands.check(predicate)

def not_monitoring_paused():
    """Check if wallet monitoring is paused"""
    async def predicate(ctx):
        if ctx.bot.monitoring_paused:
            await ctx.send("Wallet monitoring is currently unavailable. Please try again later.")
            logger.warning(f"Command rejected - monitoring paused. User: {ctx.author.id}")
            return False
        return True
    return commands.check(predicate)

def cooldown_5s():
    """5 second cooldown between commands"""
    return commands.cooldown(1, 5.0)

class RateLimiter:
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
        
        # Initialize database connection
        self.conn = None
        self.cursor = None
        self.db_lock = asyncio.Lock()
        
        # Initialize Blockfrost client
        self.blockfrost_client = None
        
        # Initialize monitoring state
        self.monitoring_paused = False
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
        # Register commands only once during initialization
        self.setup_commands()

    async def ensure_db_connection(self):
        """Ensure database connection is active"""
        try:
            if self.conn is None:
                self.conn = sqlite3.connect('wallets.db')
                self.conn.row_factory = sqlite3.Row
                self.cursor = self.conn.cursor()
                
                # Set busy timeout to avoid "database is locked" errors
                self.cursor.execute('PRAGMA busy_timeout = 30000')
                self.cursor.execute('PRAGMA journal_mode = WAL')
                self.conn.commit()
                
                logger.info("Database connection established")
                return True
                
            # Test connection
            self.cursor.execute('SELECT 1')
            return True
            
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            await self.close_db()
            return False

    async def close_db(self):
        """Close database connection"""
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.cursor = None
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}")

    async def execute_db(self, query, params=None):
        """Execute database query with automatic reconnection"""
        async with self.db_lock:
            try:
                if not await self.ensure_db_connection():
                    raise Exception("Failed to establish database connection")
                    
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                    
                self.conn.commit()
                return True
                
            except Exception as e:
                logger.error(f"Database error: {str(e)}")
                return False

    async def close(self):
        """Clean up resources when bot shuts down"""
        await self.close_db()
        await super().close()

    async def init_database(self):
        """Initialize database schema"""
        try:
            # Create tables if they don't exist
            await self.execute_db('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id TEXT UNIQUE NOT NULL
                )
            ''')
            
            await self.execute_db('''
                CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            await self.execute_db('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_id INTEGER NOT NULL,
                    tx_hash TEXT NOT NULL,
                    amount REAL NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (wallet_id) REFERENCES wallets(id)
                )
            ''')
            
            logger.info("Database schema initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {str(e)}")
            return False

    def setup_commands(self):
        """Set up bot commands using app_commands"""
        
        # Check if command is used in DM
        def dm_only():
            async def predicate(interaction: discord.Interaction) -> bool:
                if interaction.guild_id is not None:
                    try:
                        await interaction.response.send_message(
                            "❌ This command can only be used in DMs for security.",
                            ephemeral=True
                        )
                    except:
                        if not interaction.response.is_done():
                            await interaction.followup.send(
                                "❌ This command can only be used in DMs for security.",
                                ephemeral=True
                            )
                    return False
                return True
            return app_commands.check(predicate)
        
        # Add cooldowns to prevent rate limiting
        @app_commands.checks.cooldown(rate=1, per=30.0)
        @dm_only()
        @self.tree.command(name="addwallet", description="Add a wallet to monitor")
        @app_commands.describe(address="The Cardano wallet address to monitor")
        async def addwallet(interaction: discord.Interaction, address: str):
            await self.add_wallet_command(interaction, address)
        
        @app_commands.checks.cooldown(rate=1, per=30.0)
        @dm_only()
        @self.tree.command(name="removewallet", description="Remove a wallet from monitoring")
        @app_commands.describe(address="The Cardano wallet address to stop monitoring")
        async def removewallet(interaction: discord.Interaction, address: str):
            await self.remove_wallet_command(interaction, address)
        
        @app_commands.checks.cooldown(rate=1, per=30.0)
        @dm_only()
        @self.tree.command(name="listwallets", description="List all your monitored wallets")
        async def listwallets(interaction: discord.Interaction):
            await self.list_wallets_command(interaction)

        @app_commands.checks.cooldown(rate=1, per=30.0)
        @self.tree.command(name="help", description="Show help message with available commands")
        async def help(interaction: discord.Interaction):
            await self.help_command(interaction)

        @app_commands.checks.cooldown(rate=1, per=30.0)
        @self.tree.command(name="health", description="Check bot and API status")
        async def health(interaction: discord.Interaction):
            await self.health_command(interaction)

        # Add global error handler
        @self.tree.error
        async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
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

    async def setup_hook(self):
        """Called when the bot starts up"""
        try:
            # Initialize database first
            if not await self.init_database():
                raise Exception("Failed to initialize database")
            
            # Initialize Blockfrost API
            if not await self.init_blockfrost():
                logger.warning("Bot will run with limited functionality - Blockfrost API unavailable")
            
            # Sync commands with Discord
            try:
                await self.tree.sync()
                logger.info("Commands synced with Discord")
            except discord.HTTPException as e:
                logger.error(f"Failed to sync commands: {str(e)}")
                raise
            
            # Start background tasks
            self.check_wallets.start()
            logger.info("Bot setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to complete setup: {str(e)}")
            raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            # Get API key and network from environment
            api_key = os.getenv('BLOCKFROST_API_KEY')
            network = os.getenv('CARDANO_NETWORK', 'mainnet')
            
            if not api_key:
                logger.error("BLOCKFROST_API_KEY environment variable not set")
                return False
                
            # Initialize client based on network
            try:
                if network.lower() == 'mainnet':
                    self.blockfrost_client = BlockFrostApi(
                        project_id=api_key
                    )
                else:
                    self.blockfrost_client = BlockFrostApi(
                        project_id=api_key,
                        base_url="https://cardano-testnet.blockfrost.io/api/v0"
                    )
                
                # Test connection
                health = await self.blockfrost_client.health()
                if health:
                    logger.info(f"Blockfrost API initialized successfully on {network}")
                    return True
                else:
                    logger.error("Failed to verify Blockfrost API health")
                    self.blockfrost_client = None
                    return False
                    
            except Exception as e:
                logger.error(f"Failed to initialize Blockfrost client: {str(e)}")
                self.blockfrost_client = None
                return False
                
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
            self.blockfrost_client = None
            return False

    async def add_wallet_command(self, interaction: discord.Interaction, address: str):
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

    async def list_wallets_command(self, interaction: discord.Interaction):
        """Handle the listwallets command"""
        try:
            if not await self.process_interaction(interaction):
                return
                
            # Get user's wallets
            wallets = await self.execute_db(
                "SELECT * FROM wallets WHERE user_id = ? AND is_active = TRUE",
                (str(interaction.user.id),),
                fetch_all=True
            )
            
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
            
            # Check database status
            db_status = "✅ Connected" if await self.ensure_db_connection() else "❌ Not Connected"
            
            # Create status embed
            embed = discord.Embed(
                title="🔍 Bot Status",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="Bot Status", value="✅ Online", inline=True)
            embed.add_field(name="Database", value=db_status, inline=True)
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
            result = await self.execute_db(
                "SELECT last_tx_hash FROM wallets WHERE address = ? AND user_id = ?",
                (address, user_id)
            )
            last_tx = result[0] if result else None
            
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
                await self.execute_db(
                    """
                    INSERT INTO transactions (wallet_id, tx_hash, amount, block_height, timestamp)
                    VALUES ((SELECT id FROM wallets WHERE address = ? AND user_id = ?), ?, ?, ?, ?)
                    """,
                    (address, user_id, tx['hash'], tx_details['output_amount'][0]['quantity'], 
                     tx_details['block_height'], tx_details['block_time'])
                )
                
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
                await self.execute_db(
                    "UPDATE wallets SET last_tx_hash = ? WHERE address = ? AND user_id = ?",
                    (txs[0]['hash'], address, user_id)
                )
            
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
                wallets = await self.execute_db(
                    "SELECT * FROM wallets WHERE is_active = TRUE",
                    fetch_all=True
                )
                
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
            await self.execute_db(
                "UPDATE wallets SET last_checked = CURRENT_TIMESTAMP WHERE address = ? AND user_id = ?",
                (address, user_id)
            )
            
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
