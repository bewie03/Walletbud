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
from database import init_db

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

def init_db(conn):
    """Initialize SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                discord_id TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                last_tx_hash TEXT,
                last_yummi_check TIMESTAMP,
                UNIQUE(address, discord_id)
            )
        ''')
        conn.commit()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

def sanitize_wallet_address(address):
    """Sanitize and validate wallet address"""
    # Remove any whitespace
    address = address.strip()
    
    # Check if it's a valid Cardano address format
    if not address.startswith('addr1'):
        raise ValueError("Invalid Cardano address format")
        
    # Check length (Cardano addresses are ~100 chars)
    if len(address) < 90 or len(address) > 110:
        raise ValueError("Invalid Cardano address length")
        
    # Only allow alphanumeric characters
    if not address.isalnum():
        raise ValueError("Address contains invalid characters")
        
    return address

def register_command():
    """Decorator for registering slash commands"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, ctx, *args, **kwargs):
            request_id = get_request_id()
            logger.info(f"[{request_id}] Command {func.__name__} called by {ctx.author}")
            try:
                return await func(self, ctx, *args, **kwargs)
            except Exception as e:
                logger.error(f"[{request_id}] Error in command {func.__name__}: {e}")
                await ctx.send("❌ An error occurred. Please try again later.")
        return wrapper
    return decorator

class WalletModal(discord.ui.Modal, title='Add Wallet'):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.wallet = discord.ui.TextInput(
            label='Wallet Address',
            placeholder='Enter your Cardano wallet address...',
            min_length=10,
            max_length=120,
        )
        self.add_item(self.wallet)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle wallet address submission"""
        if interaction.guild is not None:
            await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
            return

        wallet_address = self.wallet.value.strip()
        try:
            wallet_address = sanitize_wallet_address(wallet_address)
        except ValueError as e:
            embed = discord.Embed(
                title="❌ Error",
                description=f"Invalid wallet address: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return

        try:
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                
                # Check if wallet already exists for this user
                cursor.execute('SELECT 1 FROM wallets WHERE address = ? AND discord_id = ?', 
                             (wallet_address, str(interaction.user.id)))
                if cursor.fetchone():
                    embed = discord.Embed(
                        title="❌ Error",
                        description="This wallet is already being monitored!",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed)
                    return

                # Check YUMMI balance before adding
                success, balance = await self.bot.check_yummi_balance(wallet_address)
                if not success:
                    embed = discord.Embed(
                        title="❌ Error",
                        description=f"Failed to check YUMMI balance: {balance}",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed)
                    return

                if balance < REQUIRED_YUMMI_TOKENS:
                    embed = discord.Embed(
                        title="❌ Insufficient YUMMI",
                        description=f"This wallet needs at least {REQUIRED_YUMMI_TOKENS:,} YUMMI tokens. Current balance: {balance:,}",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed)
                    return

                # Add wallet with transaction
                cursor.execute('''
                    INSERT INTO wallets (address, discord_id, is_active, last_yummi_check) 
                    VALUES (?, ?, TRUE, ?)
                ''', (wallet_address, str(interaction.user.id), datetime.utcnow()))
                conn.commit()

                embed = discord.Embed(
                    title="✅ Wallet Added",
                    description=f"Now monitoring wallet: `{wallet_address}`",
                    color=discord.Color.green()
                )
                embed.add_field(name="YUMMI Balance", value=f"{balance:,}", inline=False)
                await interaction.response.send_message(embed=embed)

        except sqlite3.IntegrityError:
            embed = discord.Embed(
                title="❌ Error",
                description="This wallet is already being monitored!",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            logger.error(f"Error adding wallet {wallet_address}: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description="Failed to add wallet. Please try again later.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

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
        
        # Initialize database connection
        self.conn = sqlite3.connect(DATABASE_NAME)
        self.cursor = self.conn.cursor()
        init_db(self.conn)
        
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
        @self.tree.command(name='addwallet', description="Add a wallet to monitor")
        @dm_only()
        @has_blockfrost()
        @not_monitoring_paused()
        @cooldown_5s()
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
                    # Add wallet to database
                    try:
                        self.cursor.execute(
                            "INSERT INTO wallets (address, discord_id) VALUES (?, ?)",
                            (wallet_address, str(ctx.author.id))
                        )
                        self.conn.commit()
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
        
        logger.info("Commands setup complete")

    async def setup_hook(self):
        """Called when the bot starts up"""
        try:
            logger.info("Bot is starting up...")
            
            # Initialize Blockfrost
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
            
            # Sync commands with Discord
            await self.tree.sync()
            
            logger.info("Commands registered and synced successfully")
            
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
        """Check all active wallets for new transactions"""
        if self.processing_wallets:
            logger.info("Previous wallet check still running, skipping...")
            return
            
        try:
            async with self.wallet_task_lock:
                self.processing_wallets = True
                
                # Get all active wallets
                self.cursor.execute("SELECT address, discord_id FROM wallets")
                wallets = self.cursor.fetchall()
                
                if not wallets:
                    logger.info("No wallets to check")
                    return
                    
                logger.info(f"Checking {len(wallets)} wallets for new transactions...")
                
                # Process wallets in batches of 10
                BATCH_SIZE = 10
                for i in range(0, len(wallets), BATCH_SIZE):
                    batch = wallets[i:i + BATCH_SIZE]
                    tasks = []
                    
                    # Create tasks for each wallet in batch
                    for wallet_address, discord_id in batch:
                        task = asyncio.create_task(
                            self.check_wallet_transactions(wallet_address, discord_id)
                        )
                        tasks.append(task)
                    
                    # Wait for all tasks in batch to complete
                    try:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    except Exception as e:
                        logger.error(f"Error processing batch: {str(e)}")
                    
                    # Add small delay between batches to avoid rate limits
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error in check_wallets task: {str(e)}")
        finally:
            self.processing_wallets = False

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check for new transactions in wallet"""
        request_id = get_request_id()
        try:
            # Get transactions using rate-limited request
            transactions = await self.rate_limited_request(
                self.blockfrost_client.address_transactions,
                address=wallet_address
            )
            
            if not transactions:
                logger.info(f"[{request_id}] No transactions found for {wallet_address}")
                return
                
            # Process new transactions
            new_transactions = []
            for tx in transactions:
                # Skip if we've already seen this transaction
                self.cursor.execute(
                    'SELECT last_tx_hash FROM wallets WHERE address = ? AND discord_id = ?', 
                    (wallet_address, discord_id)
                )
                result = self.cursor.fetchone()
                if result and tx.hash == result[0]:
                    break
                    
                try:
                    # Get transaction details using rate-limited request
                    tx_details = await self.rate_limited_request(
                        self.blockfrost_client.transaction,
                        hash=tx.hash
                    )
                    
                    if not tx_details:
                        logger.warning(f"[{request_id}] No details found for transaction {tx.hash}")
                        continue
                        
                    new_transactions.append({
                        'hash': tx.hash,
                        'amount': tx_details.output_amount[0].quantity if tx_details.output_amount else 0,
                        'block': tx.block_height,
                        'time': tx.block_time,
                        'explorer_url': f"https://cardanoscan.io/transaction/{tx.hash}"
                    })
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Error getting transaction details for {tx.hash}: {e}")
                    continue
                    
            # Notify about new transactions
            if new_transactions:
                try:
                    user = await self.fetch_user(int(discord_id))
                    if user:
                        for tx in new_transactions:
                            embed = discord.Embed(
                                title="🔔 New Transaction",
                                description=f"New transaction in wallet `{wallet_address}`",
                                color=discord.Color.blue(),
                                url=tx['explorer_url']  # Make title clickable
                            )
                            embed.add_field(name="Hash", value=f"[{tx['hash'][:8]}...{tx['hash'][-8:]}]({tx['explorer_url']})", inline=False)
                            embed.add_field(name="Amount", value=f"{tx['amount']:,} lovelace", inline=True)
                            embed.add_field(name="Block", value=f"[{tx['block']}](https://cardanoscan.io/block/{tx['block']})", inline=True)
                            embed.add_field(name="Time", value=datetime.fromtimestamp(tx['time']).strftime('%Y-%m-%d %H:%M:%S'), inline=True)
                            embed.add_field(
                                name="View Transaction", 
                                value=f"[View on CardanoScan]({tx['explorer_url']})\n[View on Pool.pm](https://pool.pm/tx/{tx['hash']})", 
                                inline=False
                            )
                            
                            await user.send(embed=embed)
                except Exception as e:
                    logger.error(f"[{request_id}] Error notifying user {discord_id} about transaction: {e}")
                    
            # Update last checked transaction
            if new_transactions:
                self.cursor.execute(
                    'UPDATE wallets SET last_tx_hash = ? WHERE address = ? AND discord_id = ?',
                    (new_transactions[0]['hash'], wallet_address, discord_id)
                )
                self.conn.commit()
                
        except Exception as e:
            logger.error(f"[{request_id}] Error checking transactions for wallet {wallet_address}: {e}")

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            logger.info("Initializing Blockfrost API client...")
            if not BLOCKFROST_API_KEY:
                logger.error("BLOCKFROST_API_KEY not found in environment")
                raise ValueError("BLOCKFROST_API_KEY not found in environment")
            
            # Log API key prefix for debugging (safely)
            api_key_prefix = BLOCKFROST_API_KEY[:8] if len(BLOCKFROST_API_KEY) > 8 else "***"
            logger.debug(f"Using API key prefix: {api_key_prefix}...")
            
            # Determine network from API key
            network = "mainnet"
            if BLOCKFROST_API_KEY.startswith("preprod"):
                network = "preprod"
            elif BLOCKFROST_API_KEY.startswith("preview"):
                network = "preview"
            logger.info(f"Using Blockfrost {network} network")
            
            # Initialize client
            try:
                self.blockfrost_client = BlockFrostApi(
                    project_id=BLOCKFROST_API_KEY
                )
                logger.debug("BlockFrostApi instance created")
            except Exception as e:
                logger.error(f"Failed to create BlockFrostApi instance: {str(e)}")
                raise ValueError(f"Failed to initialize Blockfrost client: {str(e)}")
            
            # Test the connection with retries
            max_retries = 3
            retry_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    logger.debug(f"Testing Blockfrost connection (attempt {attempt + 1}/{max_retries})...")
                    health = await self.rate_limited_request(
                        self.blockfrost_client.health
                    )
                    logger.debug(f"Health check response: {health}")
                    
                    # Test network info
                    network_info = await self.rate_limited_request(
                        self.blockfrost_client.network
                    )
                    logger.debug(f"Network info: {network_info}")
                    
                    logger.info("Successfully connected to Blockfrost API")
                    return True
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    logger.error(f"All connection attempts failed. Last error: {str(e)}")
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

    async def add_wallet_slash(self, interaction: discord.Interaction):
        """Slash command handler for adding a wallet"""
        try:
            # Defer the response since we'll need more than 3 seconds
            await interaction.response.defer(ephemeral=True)
            
            # Create and send the modal
            modal = WalletModal(self)
            await interaction.followup.send_modal(modal)
            
        except Exception as e:
            logger.error(f"Error in add_wallet_slash: {str(e)}")
            await interaction.followup.send(
                "An error occurred while processing your request. Please try again.",
                ephemeral=True
            )

    async def remove_wallet_slash(self, interaction: discord.Interaction):
        """Slash command handler for removing a wallet"""
        try:
            # Defer the response since we'll need more than 3 seconds
            await interaction.response.defer(ephemeral=True)
            
            # Get user's wallets from database
            user_id = str(interaction.user.id)
            self.cursor.execute(
                "SELECT wallet_address FROM wallets WHERE discord_id = ?",
                (user_id,)
            )
            wallets = self.cursor.fetchall()
            
            if not wallets:
                await interaction.followup.send(
                    "You don't have any wallets registered.",
                    ephemeral=True
                )
                return
                
            # Create a select menu with the wallets
            select = discord.ui.Select(
                placeholder="Choose a wallet to remove",
                options=[
                    discord.SelectOption(
                        label=f"Wallet {i+1}",
                        description=wallet[0][:20] + "..." if len(wallet[0]) > 20 else wallet[0],
                        value=wallet[0]
                    )
                    for i, wallet in enumerate(wallets)
                ]
            )
            
            async def select_callback(interaction: discord.Interaction):
                wallet_address = select.values[0]
                try:
                    self.cursor.execute(
                        "DELETE FROM wallets WHERE discord_id = ? AND wallet_address = ?",
                        (user_id, wallet_address)
                    )
                    self.conn.commit()
                    await interaction.response.send_message(
                        f"Wallet {wallet_address} has been removed from monitoring.",
                        ephemeral=True
                    )
                except Exception as e:
                    logger.error(f"Error removing wallet: {str(e)}")
                    await interaction.response.send_message(
                        "An error occurred while removing the wallet. Please try again.",
                        ephemeral=True
                    )
            
            select.callback = select_callback
            view = discord.ui.View()
            view.add_item(select)
            
            await interaction.followup.send(
                "Select a wallet to remove:",
                view=view,
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Error in remove_wallet_slash: {str(e)}")
            await interaction.followup.send(
                "An error occurred while processing your request. Please try again.",
                ephemeral=True
            )

    async def status_slash(self, interaction: discord.Interaction):
        """Slash command handler for checking bot status"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Check Blockfrost status
            blockfrost_status = "✅ Connected" if self.blockfrost_client else "❌ Not connected"
            
            # Get monitored wallet count
            self.cursor.execute("SELECT COUNT(*) FROM wallets")
            wallet_count = self.cursor.fetchone()[0]
            
            # Create status embed
            embed = discord.Embed(
                title="Bot Status",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Blockfrost API",
                value=blockfrost_status,
                inline=False
            )
            embed.add_field(
                name="Monitoring Status",
                value="✅ Active" if not self.monitoring_paused else "⏸️ Paused",
                inline=False
            )
            embed.add_field(
                name="Monitored Wallets",
                value=str(wallet_count),
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error in status_slash: {str(e)}")
            await interaction.followup.send(
                "An error occurred while checking status. Please try again.",
                ephemeral=True
            )

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
