import os
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
from functools import wraps

import discord
from discord import app_commands
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi

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

def init_db():
    """Initialize SQLite database"""
    try:
        with sqlite3.connect(DATABASE_NAME) as conn:
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

                if balance < REQUIRED_BUD_TOKENS:
                    embed = discord.Embed(
                        title="❌ Insufficient YUMMI",
                        description=f"This wallet needs at least {REQUIRED_BUD_TOKENS:,} YUMMI tokens. Current balance: {balance:,}",
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
        
        # Initialize core components
        self.blockfrost_client = None
        self.processing_wallets = False
        self.monitoring_paused = False
        
        # Initialize database on startup
        init_db()

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

    @tasks.loop(minutes=WALLET_CHECK_INTERVAL)
    async def check_wallets(self):
        """Check all active wallets for new transactions"""
        if self.monitoring_paused:
            logger.info("Wallet monitoring is paused")
            return

        if self.processing_wallets:
            logger.info("Already processing wallets, skipping this iteration")
            return

        try:
            self.processing_wallets = True
            logger.info("Starting wallet check cycle...")
            
            # Get all active wallets in one connection
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT w.address, w.discord_id, w.last_yummi_check 
                    FROM wallets w 
                    WHERE w.is_active = TRUE
                    ORDER BY w.last_yummi_check ASC
                ''')
                wallets = cursor.fetchall()

            if not wallets:
                logger.info("No active wallets to check")
                return

            logger.info(f"Checking {len(wallets)} wallets")
            
            for wallet_address, discord_id, last_yummi_check in wallets:
                try:
                    # Rate limit between wallet checks
                    await asyncio.sleep(1)  # 1 second between wallets to avoid rate limits
                    
                    # Check YUMMI balance if needed
                    if not last_yummi_check or \
                       datetime.utcnow() - datetime.fromisoformat(last_yummi_check) > timedelta(hours=YUMMI_CHECK_INTERVAL):
                        
                        logger.info(f"Checking YUMMI balance for wallet {wallet_address}")
                        has_tokens, message = await self.check_yummi_balance(wallet_address)
                        
                        # Update in a single transaction
                        with sqlite3.connect(DATABASE_NAME) as conn:
                            cursor = conn.cursor()
                            cursor.execute('BEGIN TRANSACTION')
                            try:
                                # Update last check time
                                cursor.execute(
                                    'UPDATE wallets SET last_yummi_check = ? WHERE address = ? AND discord_id = ?',
                                    (datetime.utcnow().isoformat(), wallet_address, discord_id)
                                )
                                
                                # Deactivate if needed
                                if not has_tokens:
                                    cursor.execute(
                                        'UPDATE wallets SET is_active = FALSE WHERE address = ? AND discord_id = ?',
                                        (wallet_address, discord_id)
                                    )
                                    conn.commit()
                                    
                                    # Notify user about deactivation
                                    try:
                                        user = await self.fetch_user(int(discord_id))
                                        if user:
                                            embed = discord.Embed(
                                                title="❌ Wallet Deactivated",
                                                description=f"Wallet `{wallet_address}` has been deactivated: {message}",
                                                color=discord.Color.red()
                                            )
                                            await user.send(embed=embed)
                                    except Exception as e:
                                        logger.error(f"Error notifying user {discord_id} about deactivation: {e}")
                                    continue
                                    
                                conn.commit()
                            except Exception as e:
                                conn.rollback()
                                logger.error(f"Error updating wallet status: {e}")
                                continue

                    # Check transactions if wallet is still active
                    await self.check_wallet_transactions(wallet_address, discord_id)
                    
                except Exception as e:
                    logger.error(f"Error processing wallet {wallet_address}: {e}")
                    
                # Rate limit between wallets
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in wallet check task: {e}")
        finally:
            self.processing_wallets = False

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check transactions for a wallet"""
        try:
            # Get wallet transactions (latest first)
            transactions = await self.blockfrost_client.address_transactions(
                address=wallet_address,
                count=10,
                order='desc'
            )
            
            if not transactions:
                return
            
            # Get last checked transaction from database
            last_tx = None
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT last_tx_hash FROM wallets WHERE address = ? AND discord_id = ?', 
                             (wallet_address, discord_id))
                result = cursor.fetchone()
                if result:
                    last_tx = result[0]
            
            # Process new transactions
            new_txs = []
            for tx in transactions:
                if not last_tx or tx.tx_hash != last_tx:
                    new_txs.append(tx)
                else:
                    break
            
            # Process transactions in chronological order (oldest first)
            for tx in reversed(new_txs):
                try:
                    # Get transaction details
                    tx_details = await self.blockfrost_client.transaction_utxos(tx.tx_hash)
                    
                    # Calculate total ADA and assets received
                    received_ada = 0
                    received_assets = {}
                    
                    for output in tx_details.outputs:
                        if output.address == wallet_address:
                            # Add ADA amount
                            received_ada += int(output.amount[0].quantity) / 1_000_000
                            
                            # Add other assets
                            for asset in output.amount[1:]:
                                asset_name = asset.unit
                                quantity = int(asset.quantity)
                                if asset_name in received_assets:
                                    received_assets[asset_name] += quantity
                                else:
                                    received_assets[asset_name] = quantity
                    
                    # Only notify if wallet received something
                    if received_ada > 0 or received_assets:
                        # Create notification message
                        message = [f"💰 New transaction for wallet `{wallet_address[:8]}...`!"]
                        
                        if received_ada > 0:
                            message.append(f"• Received: {received_ada:.6f} ADA")
                        
                        for asset_id, quantity in received_assets.items():
                            if asset_id == YUMMI_POLICY_ID:
                                message.append(f"• Received: {quantity:,} YUMMI")
                            else:
                                message.append(f"• Received: {quantity:,} of asset {asset_id}")
                        
                        message.append(f"\nTransaction: `{tx.tx_hash}`")
                        
                        # Send notification
                        user = await self.fetch_user(int(discord_id))
                        if user:
                            await user.send("\n".join(message))
                    
                except Exception as e:
                    logger.error(f"Error processing transaction {tx.tx_hash}: {e}")
                    continue
                
                # Update last checked transaction
                with sqlite3.connect(DATABASE_NAME) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'UPDATE wallets SET last_tx_hash = ? WHERE address = ? AND discord_id = ?',
                        (tx.tx_hash, wallet_address, discord_id)
                    )
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Error checking transactions for wallet {wallet_address}: {e}")

    async def check_yummi_balance(self, wallet_address):
        """Check YUMMI token balance"""
        try:
            # Get wallet's specific asset balance
            assets = await self.blockfrost_client.address_assets(
                address=wallet_address
            )
            
            if not assets:
                return True, 0  # No assets means 0 balance, but not an error
            
            # Find YUMMI token balance
            yummi_balance = 0
            for asset in assets:
                if asset.unit == YUMMI_POLICY_ID:
                    yummi_balance = int(asset.quantity)
                    break
            
            # Check if balance meets requirement
            if yummi_balance < REQUIRED_BUD_TOKENS:
                return False, f"Insufficient YUMMI balance. Required: {REQUIRED_BUD_TOKENS:,}, Current: {yummi_balance:,}"
            
            return True, yummi_balance
            
        except Exception as e:
            if "not found" in str(e).lower():
                return False, "Wallet not found on the blockchain. Please check the address and try again."
            logger.error(f"Error checking YUMMI balance: {e}")
            return False, "Failed to check YUMMI balance. Please try again later."

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                # Create Blockfrost client
                self.blockfrost_client = BlockFrostApi(
                    project_id=BLOCKFROST_API_KEY,
                    base_url=BLOCKFROST_BASE_URL
                )
                
                # Test connection with health check endpoint
                health = await self.blockfrost_client.health()
                if health.is_healthy:
                    logger.info("Blockfrost API initialized successfully")
                    return
                else:
                    raise Exception("Blockfrost API health check failed")
                
            except Exception as e:
                logger.warning(f"Blockfrost initialization attempt {attempt + 1} failed: {e}")
                self.blockfrost_client = None
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    logger.error("All Blockfrost initialization attempts failed")
                    raise

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        bot = WalletBud()
        bot.run(DISCORD_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested...")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
    finally:
        logger.info("Bot shutdown complete")
