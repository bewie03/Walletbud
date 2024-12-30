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
        # Set up intents
        intents = discord.Intents.default()
        intents.dm_messages = True     # For DM notifications
        intents.guilds = True          # Required for slash commands
        intents.message_content = True # Required for commands
        
        # Initialize the bot with intents
        super().__init__(
            command_prefix=COMMAND_PREFIX,
            intents=intents,
            help_command=None  # Disable default help command
        )
        
        # Initialize database
        try:
            init_db()  # Initialize database schema
            self.db_conn = get_db_connection()
            self.cursor = self.db_conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
        
        # Initialize locks for task management
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
        # Initialize rate limiting
        self.request_count = 0
        self.last_request_time = datetime.utcnow()
        self.rate_limit_lock = asyncio.Lock()
        
        # Initialize core components
        self.blockfrost_client = None
        self.monitoring_paused = False
        
        # Register commands
        self.setup_commands()
        
    def setup_commands(self):
        """Set up bot commands"""
        logger.info("Setting up commands...")
        
        # Register command error handler
        @self.event
        async def on_command_error(ctx, error):
            if isinstance(error, commands.NoPrivateMessage):
                await ctx.send("This command can only be used in DMs for security.")
            elif isinstance(error, commands.CheckFailure):
                # Don't send message as the check should have sent one
                pass
            else:
                logger.error(f"Command error: {str(error)}")
                await ctx.send("An error occurred while processing your command.")
        
        # Register commands
        @self.command(name='addwallet')
        @dm_only()
        @has_blockfrost()
        @not_monitoring_paused()
        async def add_wallet(ctx, wallet_address: str = None):
            """Add a wallet to monitor"""
            try:
                # Basic input validation
                if not wallet_address:
                    await ctx.send("Please provide a wallet address. Usage: !addwallet <address>")
                    return
                    
                logger.info(f"Processing add wallet request from {ctx.author.id} for address: {wallet_address}")
                
                # Check if wallet exists and get YUMMI balance
                success, result = await self.check_yummi_balance(wallet_address)
                
                if success:
                    try:
                        # Add wallet to database
                        add_wallet(wallet_address, str(ctx.author.id))
                        await ctx.send(f"Wallet added successfully! Current YUMMI balance: {result:,}")
                        logger.info(f"Wallet {wallet_address} added for user {ctx.author.id}")
                    except sqlite3.IntegrityError:
                        await ctx.send("This wallet is already being monitored!")
                        logger.warning(f"Duplicate wallet add attempt: {wallet_address}")
                else:
                    await ctx.send(result)  # Result contains error message
                    logger.warning(f"Add wallet failed - {result}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error in add_wallet command: {error_msg}")
                await ctx.send(
                    "An error occurred while processing your request. Please try again later. "
                    "If the problem persists, contact support."
                )
                
        @self.command(name='removewallet')
        @dm_only()
        async def remove_wallet(ctx, wallet_address: str = None):
            """Remove a wallet from monitoring"""
            try:
                if not wallet_address:
                    await ctx.send("Please provide a wallet address. Usage: !removewallet <address>")
                    return
                
                logger.info(f"Processing remove wallet request from {ctx.author.id} for address: {wallet_address}")
                
                try:
                    # Remove wallet from database
                    remove_wallet(wallet_address, str(ctx.author.id))
                    await ctx.send(f"Wallet removed successfully!")
                    logger.info(f"Wallet {wallet_address} removed for user {ctx.author.id}")
                except Exception as e:
                    await ctx.send("Failed to remove wallet. Make sure you own this wallet.")
                    logger.warning(f"Failed to remove wallet {wallet_address}: {str(e)}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error in remove_wallet command: {error_msg}")
                await ctx.send("An error occurred while processing your request.")
                
        @self.command(name='listwallets')
        @dm_only()
        async def list_wallets(ctx):
            """List all wallets being monitored"""
            try:
                # Get user's wallets from database
                query = '''
                    SELECT address, last_checked, is_active
                    FROM wallets
                    WHERE discord_id = ?
                    ORDER BY created_at DESC
                '''
                self.cursor.execute(query, (str(ctx.author.id),))
                wallets = self.cursor.fetchall()
                
                if not wallets:
                    await ctx.send("You don't have any wallets being monitored.")
                    return
                
                # Format wallet list
                wallet_list = ["Your monitored wallets:"]
                for wallet in wallets:
                    status = "🟢 Active" if wallet['is_active'] else "🔴 Inactive"
                    last_checked = wallet['last_checked'] or "Never"
                    wallet_list.append(
                        f"• `{wallet['address']}`\n"
                        f"  Status: {status}\n"
                        f"  Last checked: {last_checked}"
                    )
                
                await ctx.send("\n".join(wallet_list))
                logger.info(f"Listed {len(wallets)} wallets for user {ctx.author.id}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error in list_wallets command: {error_msg}")
                await ctx.send("An error occurred while retrieving your wallets.")
        
        logger.info("Commands setup complete")
        
    async def setup_hook(self):
        """Called when the bot starts up"""
        try:
            logger.info("Bot is starting up...")
            
            # Verify environment first
            await self.verify_environment()
            
            # Initialize Blockfrost client
            logger.info("Initializing Blockfrost...")
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost API")
                self.monitoring_paused = True
            
            # Start wallet monitoring if everything is ready
            if not self.monitoring_paused:
                logger.info("Starting wallet monitoring task...")
                self.check_wallets.start()
            else:
                logger.warning("Wallet monitoring is paused due to initialization errors")
                
            # Add commands
            logger.info("Registering commands...")
            await self.register_commands()
            
        except Exception as e:
            logger.error(f"Error during bot setup: {str(e)}")
            self.monitoring_paused = True
            
    async def on_ready(self):
        """Called when the bot is ready"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            logger.info(f"Using command prefix: {COMMAND_PREFIX}")
            logger.info(f"Monitoring status: {'PAUSED' if self.monitoring_paused else 'ACTIVE'}")
            
            # Log configuration
            logger.debug("Current configuration:")
            logger.debug(f"- Database: {DATABASE_NAME}")
            logger.debug(f"- YUMMI Policy ID: {YUMMI_POLICY_ID}")
            logger.debug(f"- Required YUMMI: {REQUIRED_YUMMI_TOKENS}")
            logger.debug(f"- Blockfrost initialized: {self.blockfrost_client is not None}")
            
            # Sync commands
            try:
                await self.tree.sync()
                logger.info("Commands synced globally")
            except Exception as e:
                logger.error(f"Failed to sync commands: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")
            
    async def register_commands(self):
        """Register all commands"""
        logger.info("Registering commands...")
        
        try:
            # Add command groups
            wallet_group = app_commands.Group(name="wallet", description="Wallet management commands")
            self.tree.add_command(wallet_group)
            
            # Add commands to groups
            wallet_group.add_command(app_commands.Command(
                name="add",
                description="Add a wallet to monitor",
                callback=self.add_wallet_slash
            ))
            
            wallet_group.add_command(app_commands.Command(
                name="remove",
                description="Remove a wallet from monitoring",
                callback=self.remove_wallet_slash
            ))
            
            # Add global commands
            self.tree.add_command(app_commands.Command(
                name="status",
                description="Check bot status",
                callback=self.status_slash
            ))
            
            logger.info("Commands registered successfully")
            
        except Exception as e:
            logger.error(f"Failed to register commands: {str(e)}")
            raise

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

    async def verify_environment(self):
        """Verify all required environment variables and configurations"""
        missing_vars = []
        
        # Check critical environment variables
        if not os.getenv('DISCORD_TOKEN'):
            missing_vars.append('DISCORD_TOKEN')
        if not os.getenv('BLOCKFROST_API_KEY'):
            missing_vars.append('BLOCKFROST_API_KEY')
            
        # Check database path
        db_path = os.getenv('DATABASE_NAME', 'wallets.db')
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
                        await admin_user.send(f"🚨 **Critical Error**\n{error_msg}")
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
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "ERROR": "🚨",
                "CRITICAL": "💀"
            }.get(level.upper(), "ℹ️")
            
            await admin_user.send(f"{emoji} **{level}**\n{message}")
            logger.info(f"Admin notification sent: {message}")
            
        except Exception as e:
            logger.error(f"Failed to send admin notification: {str(e)}")

    async def check_yummi_balance(self, wallet_address):
        """Check YUMMI token balance"""
        request_id = get_request_id()
        try:
            logger.info(f"[{request_id}] Checking YUMMI balance for wallet: {wallet_address}")
            logger.info(f"[{request_id}] Looking for YUMMI policy ID: {YUMMI_POLICY_ID}")
            
            if not self.blockfrost_client:
                logger.error(f"[{request_id}] Blockfrost client is not initialized")
                return False, "Bot API is not ready. Please try again in a few minutes."
            
            try:
                # Get wallet's total balance and assets using rate-limited request
                logger.debug(f"[{request_id}] Requesting address info from Blockfrost...")
                address_info = await self.rate_limited_request(
                    self.blockfrost_client.address_total,
                    address=wallet_address
                )
                logger.debug(f"[{request_id}] Raw address info response: {address_info}")
                
                if not address_info:
                    logger.warning(f"[{request_id}] No address info found for {wallet_address}")
                    return False, "Wallet not found on the blockchain. Please check:\n1. The wallet address is correct\n2. The wallet has been used at least once\n3. The Cardano network is not experiencing issues"
                
                # Find YUMMI token balance
                yummi_balance = 0
                if hasattr(address_info, 'amount'):
                    logger.info(f"[{request_id}] Found {len(address_info.amount)} assets in wallet")
                    for asset in address_info.amount:
                        # Log each asset for debugging
                        logger.debug(f"[{request_id}] Checking asset: {asset.unit} with quantity {asset.quantity}")
                        
                        # Extract policy ID and asset name from unit
                        policy_id = asset.unit[:56]  # First 56 chars are policy ID
                        asset_name_hex = asset.unit[56:]  # Rest is asset name in hex
                        logger.debug(f"[{request_id}] Policy ID: {policy_id}, Asset Name Hex: {asset_name_hex}")
                        
                        # Check if asset unit starts with YUMMI policy ID and contains "yummi" in hex
                        if policy_id == YUMMI_POLICY_ID:
                            logger.info(f"[{request_id}] Found matching policy ID!")
                            if "79756d6d69" in asset_name_hex.lower():  # "yummi" in hex
                                logger.info(f"[{request_id}] Found matching asset name!")
                                yummi_balance = int(asset.quantity)
                                logger.info(f"[{request_id}] Found YUMMI balance: {yummi_balance}")
                                break
                            else:
                                logger.debug(f"[{request_id}] Policy ID matches but asset name does not: {asset_name_hex}")
                        else:
                            logger.debug(f"[{request_id}] Policy ID does not match: {policy_id}")
                
                logger.info(f"[{request_id}] Final YUMMI balance: {yummi_balance}")
                
                # Check if balance meets requirement
                if yummi_balance < REQUIRED_YUMMI_TOKENS:
                    return False, (
                        f"Insufficient YUMMI balance. Required: {REQUIRED_YUMMI_TOKENS:,}, Current: {yummi_balance:,}\n"
                        "To get more YUMMI tokens:\n"
                        "1. Visit the YUMMI token swap page: https://app.minswap.org\n"
                        "2. Search for YUMMI token using policy ID\n"
                        f"3. Swap ADA for at least {REQUIRED_YUMMI_TOKENS:,} YUMMI tokens"
                    )
                
                return True, yummi_balance
                
            except Exception as e:
                logger.error(f"[{request_id}] Error fetching address info: {str(e)}")
                if "not found" in str(e).lower():
                    return False, (
                        "Wallet not found on the blockchain. Please check:\n"
                        "1. The wallet address is correct\n"
                        "2. The wallet has been used at least once\n"
                        "3. The Cardano network is not experiencing issues"
                    )
                raise
            
        except Exception as e:
            error_msg = (
                "Failed to check YUMMI balance. Please try:\n"
                "1. Wait a few minutes and try again\n"
                "2. Check if the Cardano network is experiencing issues\n"
                "3. If the problem persists, contact support"
            )
            logger.error(f"[{request_id}] Error checking YUMMI balance: {str(e)}")
            return False, error_msg

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
                                    f"⚠️ Your wallet `{wallet['address']}` has insufficient YUMMI tokens!\n"
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
            
    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            logger.info("Initializing Blockfrost API client...")
            if not os.getenv('BLOCKFROST_API_KEY'):
                raise ValueError("BLOCKFROST_API_KEY not found in environment")
                
            # Log the first few characters of the API key for debugging
            logger.debug(f"Using API key: {os.getenv('BLOCKFROST_API_KEY')[:8]}...")
            
            # Determine network from API key
            network = "mainnet"
            if os.getenv('BLOCKFROST_API_KEY').startswith("preprod"):
                network = "preprod"
            elif os.getenv('BLOCKFROST_API_KEY').startswith("preview"):
                network = "preview"
            logger.info(f"Using Blockfrost {network} network")
            
            # Initialize client
            try:
                self.blockfrost_client = BlockFrostApi(
                    project_id=os.getenv('BLOCKFROST_API_KEY'),
                    base_url=f"https://cardano-{network}.blockfrost.io/api/v0"
                )
                logger.debug("BlockFrostApi instance created")
            except Exception as e:
                logger.error(f"Failed to create BlockFrostApi instance: {str(e)}")
                raise ValueError("Failed to initialize Blockfrost client")
            
            # Test the connection
            try:
                logger.debug("Testing Blockfrost connection...")
                health = await self.rate_limited_request(
                    self.blockfrost_client.health
                )
                logger.debug(f"Health check response: {health}")
                
                # Also test a simple API call
                logger.debug("Testing API call...")
                network = await self.rate_limited_request(
                    self.blockfrost_client.network
                )
                logger.debug(f"Network info: {network}")
                
                logger.info("Successfully connected to Blockfrost API")
                return True
                
            except Exception as e:
                logger.error(f"Failed to connect to Blockfrost API: {str(e)}")
                if "Invalid project token" in str(e):
                    raise ValueError(
                        "Invalid Blockfrost API key. Please check your BLOCKFROST_API_KEY in .env"
                    )
                elif "Forbidden" in str(e):
                    raise ValueError(
                        "Access denied by Blockfrost. Your API key might be expired or invalid"
                    )
                elif "404" in str(e):
                    raise ValueError(
                        "API endpoint not found. Make sure you're using the correct network (mainnet/testnet)"
                    )
                raise
                
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
            self.blockfrost_client = None
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
