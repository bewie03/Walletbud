import os
import sys
import json
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from uuid import uuid4
from functools import wraps

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

class WalletBud(commands.Bot):
    def __init__(self):
        """Initialize the bot"""
        intents = discord.Intents.default()
        intents.message_content = True
        intents.dm_messages = True
        
        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None
        )
        
        # Initialize database connection with WAL mode
        try:
            self.conn = sqlite3.connect('wallets.db')
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            # Initialize database schema
            self.cursor.executescript('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id TEXT UNIQUE NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_checked TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    UNIQUE(address, user_id)
                );
            ''')
            
            # Set WAL journal mode for better concurrency
            self.cursor.execute("PRAGMA journal_mode=WAL")
            self.conn.commit()
            
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
        
        self.blockfrost_client = None
        self.monitoring_paused = False
        
        # Initialize locks for task management
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
        # Register commands only once during initialization
        self.setup_commands()

    def setup_commands(self):
        """Set up bot commands using app_commands"""
        try:
            # Add commands to the tree
            @self.tree.command(name="addwallet", description="Add a wallet to monitor")
            @app_commands.describe(address="The Cardano wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                await self.add_wallet_command(interaction, address)
            
            @self.tree.command(name="removewallet", description="Remove a wallet from monitoring")
            @app_commands.describe(address="The Cardano wallet address to stop monitoring")
            async def removewallet(interaction: discord.Interaction, address: str):
                await self.remove_wallet_command(interaction, address)
            
            @self.tree.command(name="listwallets", description="List all your monitored wallets")
            async def listwallets(interaction: discord.Interaction):
                await self.list_wallets_command(interaction)
            
            @self.tree.command(name="help", description="Show help message with available commands")
            async def help(interaction: discord.Interaction):
                await self.help_command(interaction)

            @self.tree.command(name="health", description="Check bot and API status")
            async def health(interaction: discord.Interaction):
                await self.health_command(interaction)

            # Add global error handler
            @self.tree.error
            async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
                if isinstance(error, app_commands.CommandOnCooldown):
                    await interaction.response.send_message(
                        f"Please wait {error.retry_after:.1f} seconds before using this command again.",
                        ephemeral=True
                    )
                elif isinstance(error, app_commands.CheckFailure):
                    await interaction.response.send_message(
                        "You don't have permission to use this command.",
                        ephemeral=True
                    )
                else:
                    logger.error(f"Command error: {str(error)}")
                    await interaction.response.send_message(
                        "An error occurred while processing your command.",
                        ephemeral=True
                    )
            
            logger.info("Commands registered successfully")
            
        except Exception as e:
            logger.error(f"Failed to register commands: {str(e)}")
            raise

    async def setup_hook(self):
        """Called when the bot starts up"""
        try:
            # Verify environment first
            await self.verify_environment()
            
            # Initialize Blockfrost client
            blockfrost_ok = await self.init_blockfrost()
            if not blockfrost_ok:
                logger.warning("Bot will run with limited functionality due to Blockfrost API issues")
                await self.notify_admin("Bot started with limited functionality - Blockfrost API unavailable", "WARNING")
            
            # Sync commands with Discord (only once during startup)
            try:
                await self.tree.sync()
                logger.info("Commands synced with Discord")
            except discord.HTTPException as e:
                logger.error(f"Failed to sync commands: {str(e)}")
                await self.notify_admin(f"Failed to sync commands: {str(e)}", "ERROR")
            
            # Start background tasks
            self.check_wallets.start()
            
            logger.info("Bot setup completed successfully")
            await self.notify_admin("Bot started successfully!")
            
        except Exception as e:
            error_msg = f"Failed to setup bot: {str(e)}"
            logger.critical(error_msg)
            await self.notify_admin(error_msg, "CRITICAL")
            # Don't raise here, let the bot continue

    async def on_ready(self):
        """Called when the bot is ready"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            logger.info(f"Using command prefix: !")
            logger.info(f"Monitoring status: {'PAUSED' if self.monitoring_paused else 'ACTIVE'}")
            
            # Log configuration
            logger.debug("Current configuration:")
            logger.debug(f"- Database: wallets.db")
            logger.debug(f"- YUMMI Policy ID: {YUMMI_POLICY_ID}")
            logger.debug(f"- Required YUMMI: {REQUIRED_YUMMI_TOKENS}")
            logger.debug(f"- Blockfrost initialized: {self.blockfrost_client is not None}")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")
            
    async def on_error(self, event_method: str, *args, **kwargs):
        """Global error handler for events"""
        logger.error(f'Error in {event_method}:', exc_info=True)

    async def rate_limited_request(self, method, **kwargs):
        """Handle rate limiting for Blockfrost API requests with exponential backoff"""
        request_id = get_request_id()
        max_retries = API_RETRY_ATTEMPTS
        base_delay = API_RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"[{request_id}] Making API request to {method.__name__} with args: {kwargs}")
                response = await method(**kwargs)
                logger.debug(f"[{request_id}] API request successful")
                return response
                
            except Exception as e:
                error_msg = str(e).lower()
                logger.error(f"[{request_id}] API request failed (attempt {attempt + 1}/{max_retries}): {error_msg}")
                
                if "rate limit" in error_msg:
                    wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"[{request_id}] Rate limit hit, waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    continue
                    
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"[{request_id}] Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"[{request_id}] Max retries reached. Giving up.")
                    raise

    async def check_wallet_balance(self, address: str):
        """Check wallet ADA balance"""
        try:
            if not self.blockfrost_client:
                return False, "Blockfrost API is not available"
            
            # Validate address format
            if not address.startswith(('addr1', 'addr_test1')):
                return False, "Invalid Cardano wallet address format"
                
            # Get wallet balance
            response = await self.rate_limited_request(
                self.blockfrost_client.address,
                address=address
            )
            return True, int(response.amount[0].quantity)
            
        except Exception as e:
            logger.error(f"Failed to check wallet balance: {str(e)}")
            return False, f"Failed to check wallet balance: {str(e)}"

    async def check_yummi_balance(self, address: str):
        """Check YUMMI token balance with proper error handling"""
        try:
            if not self.blockfrost_client:
                return False, "Blockfrost API is not available"
                
            # Validate address format
            if not address.startswith(('addr1', 'addr_test1')):
                return False, "Invalid Cardano wallet address format"
                
            # Get asset balance
            response = await self.rate_limited_request(
                self.blockfrost_client.address_assets,
                address=address
            )
            
            # Find YUMMI token
            for asset in response:
                if asset.unit == YUMMI_POLICY_ID:
                    return True, int(asset.quantity)
                    
            return True, 0
            
        except Exception as e:
            logger.error(f"Failed to check YUMMI balance: {str(e)}")
            return False, f"Failed to check YUMMI balance: {str(e)}"

    async def get_wallet_transactions(self, address: str, from_block: int = None):
        """Get wallet transaction history"""
        try:
            if not self.blockfrost_client:
                return False, "Blockfrost API is not available"
                
            # Validate address format
            if not address.startswith(('addr1', 'addr_test1')):
                return False, "Invalid Cardano wallet address format"
                
            # Get transactions
            response = await self.rate_limited_request(
                self.blockfrost_client.address_transactions,
                address=address,
                from_block=from_block
            )
            return True, response
            
        except Exception as e:
            logger.error(f"Failed to get wallet transactions: {str(e)}")
            return False, f"Failed to get wallet transactions: {str(e)}"

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            api_key = os.getenv('BLOCKFROST_API_KEY')
            if not api_key:
                logger.warning("BLOCKFROST_API_KEY not found")
                return False
            
            # Initialize rate limiting variables
            self.rate_limit_lock = asyncio.Lock()
            self.request_count = 0
            self.last_request_time = datetime.utcnow()
                
            # Initialize client with proper network
            network = os.getenv('CARDANO_NETWORK', 'mainnet').lower()
            base_url = "https://cardano-mainnet.blockfrost.io/api/v0" if network == 'mainnet' else "https://cardano-testnet.blockfrost.io/api/v0"
            
            self.blockfrost_client = BlockFrostApi(
                project_id=api_key,
                base_url=base_url
            )
            
            # Test the connection
            health = await self.rate_limited_request(self.blockfrost_client.health)
            if health:
                logger.info(f"Blockfrost API initialized successfully on {network}")
                return True
            else:
                logger.error("Failed to get health status from Blockfrost")
                return False
                
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
            return False

    async def add_wallet_command(self, interaction: discord.Interaction, address: str):
        """Handle the addwallet command"""
        try:
            logger.info(f"Add wallet command received from {interaction.user.id} for address {address}")
            
            # Validate address format
            if not address.startswith(('addr1', 'addr_test1')):
                await interaction.response.send_message(
                    "Invalid Cardano wallet address format. Address must start with 'addr1' or 'addr_test1'.",
                    ephemeral=True
                )
                return
                
            # Check if Blockfrost is available
            if not self.blockfrost_client:
                await interaction.response.send_message(
                    "Bot API is not ready. Please try again in a few minutes.",
                    ephemeral=True
                )
                return
                
            # Check wallet balance
            success, balance = await self.check_wallet_balance(address)
            if not success:
                await interaction.response.send_message(
                    f"Error checking wallet: {balance}",
                    ephemeral=True
                )
                return
                
            # Check YUMMI balance
            success, yummi_balance = await self.check_yummi_balance(address)
            if not success:
                await interaction.response.send_message(
                    f"Error checking YUMMI balance: {yummi_balance}",
                    ephemeral=True
                )
                return
                
            # Check minimum YUMMI requirement
            if yummi_balance < REQUIRED_YUMMI_TOKENS:
                await interaction.response.send_message(
                    f"Insufficient YUMMI tokens. Required: {REQUIRED_YUMMI_TOKENS:,}, Current: {yummi_balance:,}",
                    ephemeral=True
                )
                return
                
            try:
                # Add user if not exists
                self.cursor.execute(
                    'INSERT OR IGNORE INTO users (discord_id) VALUES (?)',
                    (str(interaction.user.id),)
                )
                
                # Get user id
                self.cursor.execute(
                    'SELECT id FROM users WHERE discord_id = ?',
                    (str(interaction.user.id),)
                )
                user_id = self.cursor.fetchone()['id']
                
                # Add wallet
                self.cursor.execute(
                    'INSERT INTO wallets (address, user_id) VALUES (?, ?)',
                    (address, user_id)
                )
                self.conn.commit()
                
                # Create success embed
                embed = discord.Embed(
                    title="✅ Wallet Added Successfully!",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                embed.add_field(name="Address", value=f"`{address}`", inline=False)
                embed.add_field(name="ADA Balance", value=f"{balance/1000000:.6f} ₳", inline=True)
                embed.add_field(name="YUMMI Balance", value=f"{yummi_balance:,}", inline=True)
                
                await interaction.response.send_message(embed=embed)
                logger.info(f"Wallet {address} added for user {interaction.user.id}")
                
            except sqlite3.IntegrityError:
                embed = discord.Embed(
                    title="❌ Wallet Already Monitored",
                    description="This wallet is already being monitored!",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                logger.warning(f"Duplicate wallet add attempt: {address}")
                
        except Exception as e:
            logger.error(f"Error in addwallet command: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing your request.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    async def remove_wallet_command(self, interaction: discord.Interaction, address: str):
        """Handle the removewallet command"""
        try:
            logger.info(f"Remove wallet command received from {interaction.user.id} for address {address}")
            
            # Validate address format
            if not address.startswith(('addr1', 'addr_test1')):
                await interaction.response.send_message(
                    "Invalid Cardano wallet address format. Address must start with 'addr1' or 'addr_test1'.",
                    ephemeral=True
                )
                return
            
            # Get user id
            self.cursor.execute(
                'SELECT id FROM users WHERE discord_id = ?',
                (str(interaction.user.id),)
            )
            user = self.cursor.fetchone()
            
            if not user:
                await interaction.response.send_message(
                    "You don't have any wallets registered.",
                    ephemeral=True
                )
                return
            
            # Remove wallet
            self.cursor.execute(
                'DELETE FROM wallets WHERE address = ? AND user_id = ?',
                (address, user['id'])
            )
            self.conn.commit()
            
            if self.cursor.rowcount > 0:
                embed = discord.Embed(
                    title="✅ Wallet Removed",
                    description=f"Successfully removed wallet: `{address}`",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                await interaction.response.send_message(embed=embed)
                logger.info(f"Wallet {address} removed for user {interaction.user.id}")
            else:
                embed = discord.Embed(
                    title="❌ Wallet Not Found",
                    description="This wallet is not in your monitoring list.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                logger.warning(f"Failed to remove wallet {address} - not found")
                
        except Exception as e:
            logger.error(f"Error in removewallet command: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing your request.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    async def list_wallets_command(self, interaction: discord.Interaction):
        """Handle the listwallets command"""
        try:
            logger.info(f"List wallets command received from {interaction.user.id}")
            
            # Get user's wallets
            self.cursor.execute('''
                SELECT w.address, w.created_at, w.last_checked
                FROM wallets w
                JOIN users u ON w.user_id = u.id
                WHERE u.discord_id = ? AND w.is_active = 1
                ORDER BY w.created_at DESC
            ''', (str(interaction.user.id),))
            
            wallets = self.cursor.fetchall()
            
            if not wallets:
                await interaction.response.send_message(
                    "You don't have any wallets being monitored.",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title="📋 Your Monitored Wallets",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            for wallet in wallets:
                # Get current balances if Blockfrost is available
                ada_balance = "Unavailable"
                yummi_balance = "Unavailable"
                
                if self.blockfrost_client:
                    success, balance = await self.check_wallet_balance(wallet['address'])
                    if success:
                        ada_balance = f"{balance/1000000:.6f} ₳"
                    
                    success, yummi = await self.check_yummi_balance(wallet['address'])
                    if success:
                        yummi_balance = f"{yummi:,}"
                
                value = f"**ADA Balance:** {ada_balance}\n**YUMMI Balance:** {yummi_balance}\n"
                value += f"**Added:** <t:{int(datetime.strptime(wallet['created_at'], '%Y-%m-%d %H:%M:%S').timestamp())}:R>\n"
                
                if wallet['last_checked']:
                    value += f"**Last Checked:** <t:{int(datetime.strptime(wallet['last_checked'], '%Y-%m-%d %H:%M:%S').timestamp())}:R>"
                
                embed.add_field(
                    name=f"`{wallet['address']}`",
                    value=value,
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed)
            logger.info(f"Listed {len(wallets)} wallets for user {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in listwallets command: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing your request.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    async def help_command(self, interaction: discord.Interaction):
        """Handle the help command"""
        try:
            logger.info(f"Help command received from {interaction.user.id}")
            
            embed = discord.Embed(
                title="🤖 WalletBud Commands",
                description="All commands must be used in DMs for security.",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            commands = [
                ("/addwallet", "Add a Cardano wallet to monitor (DM only)"),
                ("/removewallet", "Remove a wallet from monitoring (DM only)"),
                ("/listwallets", "List all your monitored wallets (DM only)"),
                ("/help", "Show this help message"),
                ("/health", "Check bot and API status")
            ]
            
            for cmd, desc in commands:
                embed.add_field(name=cmd, value=desc, inline=False)
            
            embed.add_field(
                name="ℹ️ Note",
                value="All commands must be used in DMs for security.",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed)
            logger.info(f"Help message sent to user {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in help command: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing your request.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    async def health_command(self, interaction: discord.Interaction):
        """Handle the health command"""
        try:
            logger.info(f"Health command received from {interaction.user.id}")
            
            # Check bot status
            bot_status = "✅ Bot is running"
            
            # Check Blockfrost API
            blockfrost_status = "❌ Not Connected"
            if self.blockfrost_client:
                try:
                    health = await self.rate_limited_request(self.blockfrost_client.health)
                    if health:
                        blockfrost_status = "✅ Connected"
                except Exception as e:
                    logger.error(f"Blockfrost health check failed: {str(e)}")
                    blockfrost_status = "❌ Error"
            
            # Check monitoring status
            monitoring_status = "▶️ Active" if not self.monitoring_paused else "⏸️ Paused"
            
            embed = discord.Embed(
                title="🔍 System Health Status",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="✨ Bot Status", value=bot_status, inline=False)
            embed.add_field(name="🔄 Blockfrost API", value=blockfrost_status, inline=False)
            embed.add_field(name="📊 Monitoring Status", value=monitoring_status, inline=False)
            
            await interaction.response.send_message(embed=embed)
            logger.info(f"Health status sent to user {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"Error in health command: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing your request.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    @tasks.loop(minutes=WALLET_CHECK_INTERVAL)
    async def check_wallets(self):
        """Background task to check all wallets"""
        if self.monitoring_paused:
            logger.info("Wallet monitoring is paused")
            return
            
        try:
            if not hasattr(self, 'wallet_task_lock'):
                self.wallet_task_lock = asyncio.Lock()
                self.processing_wallets = False
                logger.info("Initialized wallet task lock")
                
            async with self.wallet_task_lock:
                if self.processing_wallets:
                    logger.warning("Previous wallet check still in progress, skipping")
                    return
                    
                self.processing_wallets = True
                try:
                    # Get all active wallets
                    self.cursor.execute('''
                        SELECT w.*, u.discord_id 
                        FROM wallets w 
                        JOIN users u ON w.user_id = u.id 
                        WHERE w.is_active = 1
                    ''')
                    wallets = self.cursor.fetchall()
                    
                    if not wallets:
                        logger.info("No active wallets to check")
                        return
                        
                    logger.info(f"Checking {len(wallets)} active wallets...")
                    
                    for wallet in wallets:
                        try:
                            # Get user's DM channel
                            user = await self.fetch_user(int(wallet['discord_id']))
                            if not user:
                                logger.warning(f"Could not find user {wallet['discord_id']}")
                                continue
                                
                            dm_channel = await user.create_dm()
                            
                            # Check YUMMI balance
                            success, result = await self.check_yummi_balance(wallet['address'])
                            
                            if success:
                                # Update last checked time
                                self.cursor.execute(
                                    'UPDATE wallets SET last_checked = ? WHERE address = ?',
                                    (datetime.utcnow().isoformat(), wallet['address'])
                                )
                                self.conn.commit()
                                
                                # Send notification if balance is too low
                                if isinstance(result, int) and result < REQUIRED_YUMMI_TOKENS:
                                    await dm_channel.send(
                                        f"Your wallet `{wallet['address']}` has insufficient YUMMI tokens!\n"
                                        f"Required: {REQUIRED_YUMMI_TOKENS:,}\n"
                                        f"Current: {result:,}"
                                    )
                            else:
                                logger.warning(f"Failed to check balance for {wallet['address']}: {result}")
                                
                        except Exception as e:
                            logger.error(f"Error checking wallet {wallet['address']}: {str(e)}")
                            continue
                            
                finally:
                    self.processing_wallets = False
                    
        except Exception as e:
            logger.error(f"Error in wallet check task: {str(e)}")
            try:
                await self.notify_admin(f"Error in wallet check task: {str(e)}", "ERROR")
            except:
                pass  # Ignore notification errors
                
    async def verify_environment(self):
        """Verify all required environment variables and configurations"""
        missing_vars = []
        
        # Check critical environment variables
        if not os.getenv('DISCORD_TOKEN'):
            missing_vars.append('DISCORD_TOKEN')
        if not os.getenv('BLOCKFROST_API_KEY'):
            missing_vars.append('BLOCKFROST_API_KEY')
            
        # Check database path
        db_path = 'wallets.db'
        if not os.access(os.path.dirname(db_path) or '.', os.W_OK):
            logger.critical(f"Database path {db_path} is not writable!")
            missing_vars.append('DATABASE_NAME (not writable)')
            
        if missing_vars:
            error_msg = f"Missing or invalid environment variables: {', '.join(missing_vars)}"
            logger.critical(error_msg)
            
            # Try to notify admin if Discord token is available
            if os.getenv('DISCORD_TOKEN') and os.getenv('ADMIN_ID'):
                try:
                    admin_id = int(os.getenv('ADMIN_ID'))
                    admin_user = await self.fetch_user(admin_id)
                    if admin_user:
                        await admin_user.send(f" **Critical Error**\n{error_msg}")
                except Exception as e:
                    logger.error(f"Failed to notify admin: {str(e)}")
                    
            raise ValueError(error_msg)
            
        logger.info("Environment verification completed successfully")

    async def notify_admin(self, message, level="INFO"):
        """Notify admin of important bot events"""
        try:
            if not os.getenv('ADMIN_ID'):
                logger.warning("ADMIN_ID not set, cannot send admin notifications")
                return
                
            admin_id = int(os.getenv('ADMIN_ID'))
            admin_user = await self.fetch_user(admin_id)
            
            if not admin_user:
                logger.warning(f"Could not find admin user with ID {admin_id}")
                return
                
            # Format message based on level
            emoji = {
                "INFO": "",
                "WARNING": "",
                "ERROR": "",
                "CRITICAL": ""
            }.get(level.upper(), "")
            
            await admin_user.send(f"{emoji} **{level}**\n{message}")
            logger.info(f"Admin notification sent: {message}")
            
        except Exception as e:
            logger.error(f"Failed to send admin notification: {str(e)}")

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
