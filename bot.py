import os
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
from functools import wraps
import uuid

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi
import aiohttp

from config import *

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Centralized error messages
ERROR_MESSAGES = {
    'dm_only': "This command can only be used in DMs for security.",
    'api_unavailable': "Wallet monitoring is currently unavailable. Please try again later.",
    'monitoring_paused': "Wallet monitoring is currently paused."
}

def dm_only():
    """Ensure command is only used in DMs"""
    async def predicate(interaction: discord.Interaction):
        if interaction.guild is not None:
            await interaction.response.send_message("This command can only be used in DMs!", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

def has_blockfrost():
    """Ensure Blockfrost API is available"""
    async def predicate(interaction: discord.Interaction):
        if not interaction.client.blockfrost_client:
            await interaction.response.send_message(ERROR_MESSAGES['api_unavailable'], ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

def not_monitoring_paused():
    """Ensure wallet monitoring is not paused"""
    async def predicate(interaction: discord.Interaction):
        if interaction.client.monitoring_paused:
            await interaction.response.send_message(ERROR_MESSAGES['monitoring_paused'], ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

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

def get_request_id():
    """Generate a unique request ID"""
    return str(uuid.uuid4())[:8]

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
            intents=intents
        )
        
        # Initialize database connection
        self.conn = sqlite3.connect(DATABASE_NAME)
        self.cursor = self.conn.cursor()
        init_db(self.conn)
        
        # Initialize locks for task management
        self.wallet_task_lock = asyncio.Lock()
        self.processing_wallets = False
        
        # Initialize core components
        self.blockfrost_client = None
        self.monitoring_paused = False
        
    async def setup_hook(self):
        """Initialize the bot's command tree and sync commands"""
        try:
            # Register commands
            await self.register_commands()
            
            # Sync commands globally
            await self.tree.sync()
            logger.info("Commands synced globally")
            
            # Initialize Blockfrost
            try:
                await self.init_blockfrost()
            except Exception as e:
                logger.warning(f"Failed to initialize Blockfrost: {e}")
                logger.warning("Bot will start without Blockfrost...")
            
            logger.info("Setup complete")
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

    async def register_commands(self):
        """Register all commands with the command tree"""
        
        @self.tree.command(
            name="addwallet",
            description="Add a Cardano wallet to monitor"
        )
        @dm_only()
        @has_blockfrost()
        @not_monitoring_paused()
        async def add_wallet_command(interaction: discord.Interaction):
            modal = WalletModal(self)
            await interaction.response.send_modal(modal)

        @self.tree.command(
            name="removewallet",
            description="Remove a wallet from monitoring"
        )
        @dm_only()
        async def remove_wallet(interaction: discord.Interaction, wallet_address: str):
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM wallets WHERE address = ? AND discord_id = ?', 
                            (wallet_address, interaction.user.id))
                if cursor.rowcount > 0:
                    await interaction.response.send_message(f"✅ Wallet `{wallet_address}` removed from monitoring.")
                else:
                    await interaction.response.send_message("❌ Wallet not found in your monitored wallets.", ephemeral=True)

        @self.tree.command(
            name="listwallets",
            description="List all your monitored wallets"
        )
        @dm_only()
        async def list_wallets(interaction: discord.Interaction):
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT address FROM wallets WHERE discord_id = ?', (interaction.user.id,))
                wallets = cursor.fetchall()
                
            if wallets:
                wallet_list = "\n".join([f"• `{w[0]}`" for w in wallets])
                await interaction.response.send_message(f"Your monitored wallets:\n{wallet_list}")
            else:
                await interaction.response.send_message("You don't have any wallets being monitored.", ephemeral=True)

        @self.tree.command(
            name="help",
            description="Show bot help and commands"
        )
        async def help_command(interaction: discord.Interaction):
            help_text = (
                "🤖 **WalletBud Commands**\n\n"
                "`/addwallet` - Add a Cardano wallet to monitor (DM only)\n"
                "`/removewallet` - Remove a wallet from monitoring (DM only)\n"
                "`/listwallets` - List all your monitored wallets (DM only)\n"
                "`/help` - Show this help message\n\n"
                "ℹ️ All commands must be used in DMs for security."
            )
            await interaction.response.send_message(help_text)

        @self.tree.command(
            name="health",
            description="Check bot health status"
        )
        async def health_check(interaction: discord.Interaction):
            status = "✅ Bot is running\n"
            status += f"🔄 Blockfrost API: {'✅ Connected' if self.blockfrost_client else '❌ Not connected'}\n"
            status += f"📊 Monitoring Status: {'⏸️ Paused' if self.monitoring_paused else '▶️ Active'}"
            await interaction.response.send_message(status)

    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f'Logged in as {self.user.name} ({self.user.id})')
        
        # Sync commands globally since this is a DM-only bot
        try:
            await self.tree.sync()
            logger.info("Commands synced globally")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")
        
        # Start background tasks
        if not self.check_wallets.is_running():
            self.check_wallets.start()

    async def on_error(self, event_method: str, *args, **kwargs):
        """Global error handler for events"""
        logger.error(f'Error in {event_method}:', exc_info=True)

    async def rate_limited_request(self, method, **kwargs):
        """Handle rate limiting for Blockfrost API requests"""
        max_retries = 3
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                response = await method(**kwargs)
                return response
            except Exception as e:
                if "rate limit" in str(e).lower():
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Rate limit hit, retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                elif attempt < max_retries - 1:
                    await asyncio.sleep(base_delay)
                else:
                    raise

    async def check_yummi_balance(self, wallet_address):
        """Check YUMMI token balance"""
        request_id = get_request_id()
        try:
            logger.info(f"[{request_id}] Checking YUMMI balance for wallet: {wallet_address}")
            logger.info(f"[{request_id}] Looking for YUMMI policy ID: {YUMMI_POLICY_ID}")
            
            try:
                # Get wallet's total balance and assets using rate-limited request
                address_info = await self.rate_limited_request(
                    self.blockfrost_client.address_total,
                    address=wallet_address
                )
                logger.info(f"[{request_id}] Received address info: {address_info}")
                
                if not address_info:
                    logger.warning(f"[{request_id}] No address info found for {wallet_address}")
                    return False, "Wallet not found on the blockchain. Please check:\n1. The wallet address is correct\n2. The wallet has been used at least once\n3. The Cardano network is not experiencing issues"
                
                # Find YUMMI token balance
                yummi_balance = 0
                if hasattr(address_info, 'amount'):
                    logger.info(f"[{request_id}] Found {len(address_info.amount)} assets in wallet")
                    for asset in address_info.amount:
                        # Log each asset for debugging
                        logger.info(f"[{request_id}] Checking asset: {asset.unit} with quantity {asset.quantity}")
                        
                        # Extract policy ID and asset name from unit
                        policy_id = asset.unit[:56]  # First 56 chars are policy ID
                        asset_name_hex = asset.unit[56:]  # Rest is asset name in hex
                        logger.info(f"[{request_id}] Policy ID: {policy_id}, Asset Name Hex: {asset_name_hex}")
                        
                        # Check if asset unit starts with YUMMI policy ID and contains "yummi" in hex
                        if policy_id == YUMMI_POLICY_ID:
                            logger.info(f"[{request_id}] Found matching policy ID!")
                            if "79756d6d69" in asset_name_hex.lower():  # "yummi" in hex
                                logger.info(f"[{request_id}] Found matching asset name!")
                                yummi_balance = int(asset.quantity)
                                logger.info(f"[{request_id}] Found YUMMI balance: {yummi_balance}")
                                break
                            else:
                                logger.info(f"[{request_id}] Policy ID matches but asset name does not: {asset_name_hex}")
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
            logger.error(f"[{request_id}] {str(e)}")
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
            
            # Get API key and verify it exists
            api_key = os.getenv('BLOCKFROST_API_KEY')
            if not api_key:
                logger.error("Blockfrost API key is not set!")
                return False
                
            # Log first few characters of key (safely)
            logger.info(f"Using Blockfrost API key: {api_key[:4]}...")
            
            # Initialize client with project_id (API key)
            self.blockfrost_client = BlockFrostApi(
                project_id=api_key
            )
            
            # Test connection with health check
            try:
                health = self.blockfrost_client.health()
                logger.info(f"Blockfrost health check: {health}")
                
                if not health.is_healthy:
                    logger.error("Blockfrost API health check failed")
                    return False
                    
                logger.info("Blockfrost API initialized successfully")
                return True
                
            except Exception as e:
                logger.error(f"Blockfrost health check failed: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {str(e)}")
            return False

    @commands.command(name='addwallet')
    @register_command()
    @commands.cooldown(1, 5.0)
    async def add_wallet(self, ctx, wallet_address: str):
        """Add a wallet to monitor"""
        try:
            # Sanitize and validate the wallet address
            try:
                wallet_address = sanitize_wallet_address(wallet_address)
            except ValueError as e:
                await ctx.send(f"❌ Invalid wallet address: {str(e)}")
                return
                
            discord_id = str(ctx.author.id)
            
            # Check YUMMI balance first
            has_tokens, message = await self.check_yummi_balance(wallet_address)
            if not has_tokens:
                await ctx.send(f"❌ {message}")
                return
                
            # Use parameterized query to prevent SQL injection
            self.cursor.execute(
                'INSERT OR REPLACE INTO wallets (address, discord_id, is_active, last_yummi_check) VALUES (?, ?, TRUE, ?)',
                (wallet_address, discord_id, datetime.utcnow().isoformat())
            )
            self.conn.commit()
            
            await ctx.send(f"✅ Wallet `{wallet_address}` added successfully! YUMMI balance: {message:,}")
            
        except Exception as e:
            logger.error(f"Error adding wallet: {e}")
            await ctx.send("❌ Failed to add wallet. Please try again later.")

    @commands.command(name='removewallet')
    @register_command()
    @commands.cooldown(1, 5.0)
    async def remove_wallet(self, ctx, wallet_address: str):
        """Remove a wallet from monitoring"""
        try:
            # Sanitize and validate the wallet address
            try:
                wallet_address = sanitize_wallet_address(wallet_address)
            except ValueError as e:
                await ctx.send(f"❌ Invalid wallet address: {str(e)}")
                return
                
            discord_id = str(ctx.author.id)
            
            # Use parameterized query to prevent SQL injection
            self.cursor.execute(
                'DELETE FROM wallets WHERE address = ? AND discord_id = ?',
                (wallet_address, discord_id)
            )
            self.conn.commit()
            
            if self.cursor.rowcount > 0:
                await ctx.send(f"✅ Wallet `{wallet_address}` removed successfully!")
            else:
                await ctx.send(f"❌ Wallet `{wallet_address}` not found or not owned by you.")
            
        except Exception as e:
            logger.error(f"Error removing wallet: {e}")
            await ctx.send("❌ Failed to remove wallet. Please try again later.")

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        bot = WalletBud()
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        # Cleanup resources
        if hasattr(bot, 'conn'):
            bot.conn.close()
            logger.info("Database connection closed")
        logger.info("Bot shutdown complete")
