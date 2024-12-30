import os
import sys
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi, ApiError
from dotenv import load_dotenv

from config import *

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
        request_id = get_request_id()
        logger.info(f"[{request_id}] Processing wallet submission from {interaction.user.id}")
        
        # Validate wallet address
        wallet_address = self.wallet.value.strip()
        try:
            if not wallet_address.startswith('addr1'):
                raise ValueError("Invalid Cardano address format")
                
            if len(wallet_address) < 90 or len(wallet_address) > 110:
                raise ValueError("Invalid Cardano address length")
                
            if not wallet_address.isalnum():
                raise ValueError("Address contains invalid characters")
        except ValueError as e:
            logger.warning(f"[{request_id}] Invalid wallet address: {str(e)}")
            await interaction.response.send_message(f"❌ {str(e)}", ephemeral=True)
            return

        try:
            # Check if wallet already exists for this user
            self.bot.cursor.execute(
                'SELECT 1 FROM wallets WHERE address = ? AND discord_id = ?', 
                (wallet_address, str(interaction.user.id))
            )
            if self.bot.cursor.fetchone():
                await interaction.response.send_message("❌ You are already monitoring this wallet", ephemeral=True)
                return
                
            # Check if wallet is monitored by another user
            self.bot.cursor.execute(
                'SELECT discord_id FROM wallets WHERE address = ?', 
                (wallet_address,)
            )
            result = self.bot.cursor.fetchone()
            if result:
                await interaction.response.send_message("❌ This wallet is already being monitored by another user", ephemeral=True)
                return
                
            # Check YUMMI balance
            try:
                balance = await self.bot.check_yummi_balance(wallet_address)
                if not balance or balance < REQUIRED_YUMMI_TOKENS:
                    await interaction.response.send_message(
                        f"❌ Insufficient YUMMI tokens. Required: {REQUIRED_YUMMI_TOKENS:,}, Current: {balance:,}",
                        ephemeral=True
                    )
                    return
            except Exception as e:
                logger.error(f"[{request_id}] Error checking YUMMI balance: {str(e)}")
                await interaction.response.send_message("❌ Error checking wallet balance. Please try again later.", ephemeral=True)
                return
                
            # Add wallet to database
            try:
                self.bot.cursor.execute(
                    'INSERT INTO wallets (address, discord_id, is_active, last_checked) VALUES (?, ?, TRUE, ?)',
                    (wallet_address, str(interaction.user.id), datetime.utcnow())
                )
                self.bot.conn.commit()
                
                embed = discord.Embed(
                    title="✅ Wallet Added",
                    description=f"Now monitoring wallet: `{wallet_address}`\nCurrent YUMMI balance: {balance:,}",
                    color=discord.Color.green()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                logger.info(f"[{request_id}] Wallet {wallet_address} added for user {interaction.user.id}")
                
            except sqlite3.IntegrityError:
                logger.error(f"[{request_id}] Database integrity error adding wallet")
                await interaction.response.send_message("❌ Error adding wallet. Please try again later.", ephemeral=True)
            except Exception as e:
                logger.error(f"[{request_id}] Error adding wallet to database: {str(e)}")
                await interaction.response.send_message("❌ Error adding wallet. Please try again later.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"[{request_id}] Error in wallet submission: {str(e)}")
            await interaction.response.send_message("❌ An error occurred. Please try again later.", ephemeral=True)

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
            # Get database path
            if os.getenv('DATABASE_URL'):
                db_path = os.getenv('DATABASE_URL')
            else:
                # Use relative path from current directory
                current_dir = os.path.dirname(os.path.abspath(__file__))
                db_path = os.path.join(current_dir, DATABASE_NAME)
                
            logger.info(f"Using database at: {db_path}")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Initialize connection
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            self.init_db()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise

        # Initialize other attributes
        self.blockfrost_client = None
        self.monitoring_paused = False
        
        # Setup commands
        self.setup_commands()

    async def setup_hook(self):
        """Called when the bot starts up"""
        try:
            logger.info("Bot is starting up...")

            # Initialize Blockfrost first
            logger.info("Initializing Blockfrost...")
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost API")
                self.monitoring_paused = True
            
            # Sync commands with Discord
            try:
                logger.info("Registering commands...")
                await self.tree.sync()
                command_list = [cmd.name for cmd in self.tree.get_commands()]
                logger.info(f"Commands synced successfully: {command_list}")
            except Exception as e:
                logger.error(f"Error syncing commands: {e}")
                raise

            # Start wallet monitoring if everything is ready
            if not self.monitoring_paused:
                logger.info("Starting wallet monitoring task...")
                self.check_wallets.start()
            else:
                logger.warning("Wallet monitoring is paused due to initialization errors")

        except Exception as e:
            logger.error(f"Error during bot setup: {e}")
            self.monitoring_paused = True
            raise

    def setup_commands(self):
        """Set up bot commands"""
        logger.info("Setting up commands...")
        
        # Add health check command
        @self.tree.command(name="health", description="Check bot health")
        async def health(interaction: discord.Interaction):
            command_list = [cmd.name for cmd in self.tree.get_commands()]
            await interaction.response.send_message(
                f"Bot is running!\nRegistered commands: {', '.join(command_list)}", 
                ephemeral=True
            )

        # Add wallet commands
        @self.tree.command(name="addwallet", description="Add a wallet to monitor")
        @app_commands.check(dm_only)
        @app_commands.check(has_blockfrost)
        @app_commands.check(not_monitoring_paused)
        async def addwallet(interaction: discord.Interaction):
            await self.cmd_addwallet(interaction)
            
        @self.tree.command(name="removewallet", description="Stop monitoring a wallet")
        @app_commands.check(dm_only)
        async def removewallet(interaction: discord.Interaction, address: str):
            await self.cmd_removewallet(interaction, address)
                
        @self.tree.command(name="listwallets", description="List your monitored wallets")
        @app_commands.check(dm_only)
        async def listwallets(interaction: discord.Interaction):
            await self.cmd_listwallets(interaction)
        
        @self.tree.command(name="status", description="Check bot status")
        async def status(interaction: discord.Interaction):
            await self.cmd_status(interaction)
            
        logger.info(f"Commands setup complete! Registered: {[cmd.name for cmd in self.tree.get_commands()]}")

    # Command callbacks
    async def cmd_addwallet(self, interaction: discord.Interaction):
        """Add a wallet to monitor"""
        await interaction.response.send_modal(WalletModal(self))
        
    async def cmd_removewallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            self.cursor.execute(
                'DELETE FROM wallets WHERE address = ? AND discord_id = ?',
                (address, str(interaction.user.id))
            )
            self.conn.commit()
            await interaction.response.send_message("✅ Wallet removed from monitoring", ephemeral=True)
        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            await interaction.response.send_message("❌ Failed to remove wallet", ephemeral=True)
            
    async def cmd_listwallets(self, interaction: discord.Interaction):
        """List monitored wallets"""
        try:
            self.cursor.execute(
                'SELECT address FROM wallets WHERE discord_id = ?',
                (str(interaction.user.id),)
            )
            wallets = self.cursor.fetchall()
            
            if not wallets:
                await interaction.response.send_message("You are not monitoring any wallets", ephemeral=True)
                return
                
            embed = discord.Embed(
                title="Your Monitored Wallets",
                color=discord.Color.blue()
            )
            for i, (address,) in enumerate(wallets, 1):
                embed.add_field(
                    name=f"Wallet {i}",
                    value=f"`{address}`",
                    inline=False
                )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            await interaction.response.send_message("❌ Failed to list wallets", ephemeral=True)

    async def cmd_status(self, interaction: discord.Interaction):
        """Check bot status"""
        embed = discord.Embed(
            title="Bot Status",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Monitoring Status",
            value="Active" if not self.monitoring_paused else "Paused",
            inline=False
        )
        embed.add_field(
            name="Blockfrost API",
            value="Ready" if self.blockfrost_client else "Not Ready",
            inline=False
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    def init_db(self):
        """Initialize database tables"""
        try:
            # Create wallets table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS wallets (
                    address TEXT PRIMARY KEY,
                    discord_id TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_tx_hash TEXT,
                    last_checked TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(address, discord_id)
                )
            ''')
            self.conn.commit()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {str(e)}")
            raise
        
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
            
        if not self.blockfrost_client:
            logger.error("Blockfrost client not initialized, skipping wallet check")
            return
            
        if self.monitoring_paused:
            logger.warning("Wallet monitoring is paused, skipping check")
            return
            
        try:
            async with self.wallet_task_lock:
                self.processing_wallets = True
                
                # Get all active wallets
                try:
                    self.cursor.execute("SELECT address, discord_id FROM wallets")
                    wallets = self.cursor.fetchall()
                except sqlite3.Error as e:
                    logger.error(f"Database error in check_wallets: {str(e)}")
                    return
                
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
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        # Check for errors in batch
                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"Error in wallet check batch: {str(result)}")
                    except Exception as e:
                        logger.error(f"Error processing batch: {str(e)}")
                    
                    # Add small delay between batches to avoid rate limits
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error in check_wallets task: {str(e)}")
            # Don't let a single error pause monitoring
            if "rate limit" not in str(e).lower():
                self.monitoring_paused = True
        finally:
            self.processing_wallets = False

    @check_wallets.before_loop
    async def before_check_wallets(self):
        """Wait for bot to be ready before starting wallet checks"""
        await self.wait_until_ready()
        logger.info("Starting wallet monitoring task...")

    @check_wallets.error
    async def check_wallets_error(self, error):
        """Handle errors in the wallet check task"""
        logger.error(f"Wallet check task error: {str(error)}")
        self.processing_wallets = False  # Reset flag
        
        if "rate limit" in str(error).lower():
            logger.warning("Rate limit hit, will retry next interval")
        else:
            logger.error("Critical error in wallet monitoring task")
            self.monitoring_paused = True

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check for new transactions in wallet"""
        request_id = get_request_id()
        try:
            if not self.blockfrost_client:
                logger.error(f"[{request_id}] Blockfrost client not initialized")
                return
                
            # Get transactions using rate-limited request
            try:
                transactions = await self.rate_limited_request(
                    self.blockfrost_client.address_transactions,
                    address=wallet_address
                )
            except Exception as e:
                logger.error(f"[{request_id}] Failed to get transactions for {wallet_address}: {str(e)}")
                if "rate limit" in str(e).lower():
                    # Don't treat rate limits as critical errors
                    return
                raise
            
            if not transactions:
                logger.info(f"[{request_id}] No transactions found for {wallet_address}")
                return
                
            # Get last checked transaction
            try:
                self.cursor.execute(
                    'SELECT last_tx_hash FROM wallets WHERE address = ? AND discord_id = ?', 
                    (wallet_address, discord_id)
                )
                result = self.cursor.fetchone()
                last_tx_hash = result[0] if result else None
            except sqlite3.Error as e:
                logger.error(f"[{request_id}] Database error getting last transaction: {str(e)}")
                return
                
            # Process new transactions
            new_transactions = []
            for tx in transactions:
                # Skip if we've already seen this transaction
                if last_tx_hash and tx.hash == last_tx_hash:
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
                    logger.error(f"[{request_id}] Error getting transaction details for {tx.hash}: {str(e)}")
                    if "rate limit" in str(e).lower():
                        # Stop processing if we hit rate limit
                        return
                    continue
                    
            # Notify about new transactions
            if new_transactions:
                try:
                    user = await self.fetch_user(int(discord_id))
                    if not user:
                        logger.error(f"[{request_id}] Could not find user {discord_id}")
                        return
                        
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
                        
                        try:
                            await user.send(embed=embed)
                        except discord.errors.Forbidden:
                            logger.error(f"[{request_id}] Cannot send DM to user {discord_id}")
                            return
                        except Exception as e:
                            logger.error(f"[{request_id}] Error sending notification to {discord_id}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"[{request_id}] Error notifying user {discord_id} about transaction: {str(e)}")
                    return
                    
                # Update last checked transaction
                try:
                    self.cursor.execute(
                        'UPDATE wallets SET last_tx_hash = ? WHERE address = ? AND discord_id = ?',
                        (new_transactions[0]['hash'], wallet_address, discord_id)
                    )
                    self.conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"[{request_id}] Database error updating last transaction: {str(e)}")
                    
        except Exception as e:
            logger.error(f"[{request_id}] Error checking transactions for wallet {wallet_address}: {str(e)}")
            # Only raise if it's not a rate limit error
            if "rate limit" not in str(e).lower():
                raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        if not BLOCKFROST_API_KEY:
            logger.error("BLOCKFROST_API_KEY is missing")
            return False  # Explicitly return False if missing API key
            
        try:
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_API_KEY
            )
            # Check connection
            await self.blockfrost_client.health()
            logger.info("Blockfrost API initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Blockfrost: {e}")
            self.blockfrost_client = None  # Reset on error
            return False

    async def close(self):
        """Cleanup resources"""
        if hasattr(self, 'conn'):
            self.conn.close()
            logger.info("Database connection closed")
        await super().close()

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
