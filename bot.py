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
            self.cursor.execute("PRAGMA journal_mode=WAL")
            self.conn.commit()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
        
        self.blockfrost_client = None
        self.monitoring_paused = False
        
        # Register commands only once during initialization
        self.setup_commands()
        
    def setup_commands(self):
        """Set up bot commands using app_commands"""
        try:
            # Add commands to the tree
            @self.tree.command(name="addwallet", description="Add a wallet to monitor")
            @app_commands.describe(address="The Cardano wallet address to monitor")
            async def addwallet(interaction: discord.Interaction, address: str):
                if interaction.guild is not None:
                    await interaction.response.send_message("This command can only be used in DMs.", ephemeral=True)
                    return
                await self.add_wallet_command(interaction, address)
            
            @self.tree.command(name="removewallet", description="Remove a wallet from monitoring")
            @app_commands.describe(address="The Cardano wallet address to stop monitoring")
            async def removewallet(interaction: discord.Interaction, address: str):
                if interaction.guild is not None:
                    await interaction.response.send_message("This command can only be used in DMs.", ephemeral=True)
                    return
                await self.remove_wallet_command(interaction, address)
            
            @self.tree.command(name="listwallets", description="List all your monitored wallets")
            async def listwallets(interaction: discord.Interaction):
                if interaction.guild is not None:
                    await interaction.response.send_message("This command can only be used in DMs.", ephemeral=True)
                    return
                await self.list_wallets_command(interaction)
            
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
        """Handle rate limiting for Blockfrost API requests"""
        request_id = get_request_id()
        max_retries = 3
        
        async with self.rate_limit_lock:
            current_time = datetime.utcnow()
            time_diff = (current_time - self.last_request_time).total_seconds()
            
            # Reset counter if more than 50 seconds have passed
            if time_diff > 50:
                logger.debug(f"[{request_id}] Resetting rate limit counter after {time_diff} seconds")
                self.request_count = 0
            
            # If we've hit the burst limit, wait
            if self.request_count >= 500:
                wait_time = 50 - time_diff
                if wait_time > 0:
                    logger.warning(f"[{request_id}] Rate limit burst reached, waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    self.request_count = 0
            
            # If we're making requests too fast, wait
            if time_diff < 0.1:  # Ensure at least 100ms between requests
                wait_time = 0.1 - time_diff
                logger.debug(f"[{request_id}] Request too fast, waiting {wait_time} seconds")
                await asyncio.sleep(wait_time)
            
            # Update counters
            self.request_count += 1
            self.last_request_time = current_time
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"[{request_id}] Making API request to {method.__name__} with args: {kwargs}")
                response = await method(**kwargs)
                logger.debug(f"[{request_id}] API response received")
                return response
                
            except Exception as e:
                logger.error(f"[{request_id}] API request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                
                if "rate limit" in str(e).lower():
                    # If we hit the rate limit, wait longer
                    wait_time = 60 if attempt == 0 else 120
                    logger.warning(f"[{request_id}] Rate limit hit, waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    continue
                    
                if attempt < max_retries - 1:
                    delay = 1 * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    logger.info(f"[{request_id}] Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[{request_id}] Max retries reached. Giving up.")
                    raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            api_key = os.getenv('BLOCKFROST_API_KEY')
            if not api_key:
                logger.warning("BLOCKFROST_API_KEY not found")
                return False
                
            self.blockfrost_client = BlockFrostApi(
                project_id=api_key,
            )
            # Test the connection
            await self.blockfrost_client.health()
            logger.info("Blockfrost API initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
            # Don't raise, let the bot continue with warnings
            return False

    def execute_db_query(self, query, params=None):
        """Execute a database query with proper error handling"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            return False

    async def check_yummi_balance(self, address):
        """Check YUMMI token balance with proper error handling"""
        try:
            if not self.blockfrost_client:
                return False, "Blockfrost API is not available"
                
            # Get asset balance
            result = await self.blockfrost_client.address_assets(address)
            
            # Find YUMMI token
            for asset in result:
                if asset.unit == YUMMI_POLICY_ID:
                    return True, int(asset.quantity)
                    
            return True, 0
            
        except Exception as e:
            logger.error(f"Failed to check YUMMI balance: {str(e)}")
            return False, "Failed to check wallet balance. Please try again later."

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

    @tasks.loop(minutes=WALLET_CHECK_INTERVAL)
    async def check_wallets(self):
        """Background task to check all wallets"""
        while not self.is_closed():
            try:
                async with self.wallet_task_lock:
                    if self.processing_wallets:
                        logger.warning("Wallet check already in progress, skipping...")
                        continue
                        
                    self.processing_wallets = True
                    
                try:
                    # Get all active wallets
                    self.cursor.execute('''
                        SELECT w.id, w.address, w.discord_id, w.last_checked
                        FROM wallets w
                        WHERE w.is_active = TRUE
                        ORDER BY w.last_checked ASC NULLS FIRST
                        LIMIT ?
                    ''', (WALLET_BATCH_SIZE,))
                    
                    wallets = self.cursor.fetchall()
                    
                    if not wallets:
                        logger.info("No active wallets to check")
                        continue
                    
                    logger.info(f"Checking {len(wallets)} wallets...")
                    
                    # Track API requests for rate limiting
                    request_count = 0
                    rate_limit_reached = False
                    
                    for wallet in wallets:
                        try:
                            # Check if we're approaching rate limit
                            if request_count >= MAX_REQUESTS_PER_SECOND:
                                rate_limit_reached = True
                                await self.notify_admin(
                                    "Rate limit threshold reached, pausing wallet checks",
                                    "WARNING"
                                )
                                break
                                
                            # Check YUMMI balance
                            success, result = await self.check_yummi_balance(wallet['address'])
                            request_count += 1
                            
                            # Update last checked time
                            update_last_checked(wallet['id'])
                            
                            if not success:
                                logger.warning(f"Failed to check wallet {wallet['address']}: {result}")
                                continue
                                
                            # Get user's DM channel
                            user = await self.fetch_user(int(wallet['discord_id']))
                            if not user:
                                logger.warning(f"Could not find user {wallet['discord_id']}")
                                continue
                                
                            dm_channel = await user.create_dm()
                            
                            # Send notification if balance is too low
                            if isinstance(result, int) and result < REQUIRED_YUMMI_TOKENS:
                                await dm_channel.send(
                                    f" Your wallet `{wallet['address']}` has insufficient YUMMI tokens!\n"
                                    f"Required: {REQUIRED_YUMMI_TOKENS:,}\n"
                                    f"Current: {result:,}"
                                )
                            
                        except Exception as e:
                            error_msg = f"Error checking wallet {wallet['address']}: {str(e)}"
                            logger.error(error_msg)
                            await self.notify_admin(error_msg, "ERROR")
                            continue
                            
                    if rate_limit_reached:
                        # Add extra delay if we hit rate limit
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                        
                finally:
                    self.processing_wallets = False
                    
            except Exception as e:
                error_msg = f"Error in wallet check task: {str(e)}"
                logger.error(error_msg)
                await self.notify_admin(error_msg, "ERROR")
                
            # Wait before next check
            await asyncio.sleep(WALLET_CHECK_INTERVAL)
            
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
