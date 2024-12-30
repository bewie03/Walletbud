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
    """Decorator to ensure command is only used in DMs"""
    async def predicate(interaction: discord.Interaction):
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(ERROR_MESSAGES['dm_only'], ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

def cooldown_5s():
    """5 second cooldown between commands"""
    return app_commands.cooldown(1, 5.0)

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
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(ERROR_MESSAGES['dm_only'], ephemeral=True)
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
            # Initialize Blockfrost API (non-blocking)
            try:
                await self.init_blockfrost()
            except Exception as e:
                logger.warning(f"Failed to initialize Blockfrost: {e}")
                logger.warning("Bot will start without Blockfrost...")
            
            # Register commands - using the existing command tree
            self.register_commands()
            logger.info("Setup complete")
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

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

    def register_commands(self):
        """Register all commands with the command tree"""
        
        @self.tree.command(name="addwallet", description="Add a Cardano wallet for tracking")
        @dm_only()
        @has_blockfrost()
        @not_monitoring_paused()
        @cooldown_5s()
        async def add_wallet_command(interaction: discord.Interaction):
            modal = WalletModal(self)
            await interaction.response.send_modal(modal)

        @self.tree.command(name="removewallet", description="Remove a wallet from monitoring")
        @dm_only()
        @cooldown_5s()
        async def remove_wallet(interaction: discord.Interaction, wallet_address: str):
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT discord_id FROM wallets WHERE address = ?', (wallet_address,))
                result = cursor.fetchone()

                if not result:
                    embed = discord.Embed(
                        title="❌ Error",
                        description="This wallet is not being monitored!",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                    return

                if str(interaction.user.id) != result[0]:
                    embed = discord.Embed(
                        title="❌ Error",
                        description="You can only remove wallets that you added!",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                    return

                cursor.execute('DELETE FROM wallets WHERE address = ?', (wallet_address,))
                conn.commit()

                embed = discord.Embed(
                    title="✅ Success",
                    description=f"Wallet `{wallet_address}` has been removed from monitoring.",
                    color=discord.Color.green()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)

        @self.tree.command(name="listwallets", description="List all monitored wallets")
        @dm_only()
        @cooldown_5s()
        async def list_wallets(interaction: discord.Interaction):
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT address, is_active FROM wallets WHERE discord_id = ?', (str(interaction.user.id),))
                wallets = cursor.fetchall()

            if not wallets:
                embed = discord.Embed(
                    title="📋 Your Wallets",
                    description="You don't have any wallets being monitored.",
                    color=discord.Color.blue()
                )
            else:
                embed = discord.Embed(
                    title="📋 Your Wallets",
                    description=f"You have {len(wallets)} wallet(s) being monitored:",
                    color=discord.Color.blue()
                )
                for address, is_active in wallets:
                    status = "🟢 Active" if is_active else "🔴 Inactive"
                    embed.add_field(
                        name=f"{status}",
                        value=f"`{address}`",
                        inline=False
                    )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        @self.tree.command(name="help", description="Show available commands")
        @cooldown_5s()
        async def help_command(interaction: discord.Interaction):
            embed = discord.Embed(
                title="WalletBud Commands",
                description="Here are all available commands:",
                color=discord.Color.blue()
            )
        
            embed.add_field(
                name="/addwallet",
                value="Add a Cardano wallet to monitor (DM only)",
                inline=False
            )
            embed.add_field(
                name="/removewallet",
                value="Remove a wallet from monitoring (DM only)",
                inline=False
            )
            embed.add_field(
                name="/listwallets",
                value="List all your monitored wallets (DM only)",
                inline=False
            )
            embed.add_field(
                name="/health",
                value="Check bot health status",
                inline=False
            )
        
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @self.tree.command(name="health", description="Check bot health status")
        @cooldown_5s()
        async def health_check(interaction: discord.Interaction):
            embed = discord.Embed(
                title="Bot Health Status",
                color=discord.Color.blue()
            )
        
            # Check Blockfrost API
            api_status = "🟢 Connected" if self.blockfrost_client else "🔴 Disconnected"
            embed.add_field(
                name="Blockfrost API",
                value=api_status,
                inline=False
            )
        
            # Check monitoring status
            monitor_status = "🔴 Paused" if self.monitoring_paused else "🟢 Active"
            embed.add_field(
                name="Wallet Monitoring",
                value=monitor_status,
                inline=False
            )
        
            # Get total wallets
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM wallets')
                total_wallets = cursor.fetchone()[0]
                cursor.execute('SELECT COUNT(*) FROM wallets WHERE is_active = 1')
                active_wallets = cursor.fetchone()[0]
        
            embed.add_field(
                name="Monitored Wallets",
                value=f"Total: {total_wallets}\nActive: {active_wallets}",
                inline=False
            )
        
            await interaction.response.send_message(embed=embed, ephemeral=True)

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
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT last_tx_hash FROM wallets WHERE address = ? AND discord_id = ?', 
                             (wallet_address, discord_id))
                last_tx = cursor.fetchone()
                
                if not last_tx:
                    logger.warning(f"Wallet {wallet_address} not found for user {discord_id}")
                    return

                if not last_tx[0]:  # No previous transactions
                    logger.info(f"No previous transactions for wallet {wallet_address}")
                    return

                transactions = []
                try:
                    async for tx in self.blockfrost_client.address_transactions_all(wallet_address):
                        transactions.append(tx)
                        if len(transactions) >= MAX_TX_HISTORY:
                            break
                except Exception as e:
                    logger.error(f"Error fetching transactions from Blockfrost: {e}")
                    return

                if not transactions:
                    logger.info(f"No new transactions for wallet {wallet_address}")
                    return

                # Update last transaction hash
                cursor.execute(
                    'UPDATE wallets SET last_tx_hash = ? WHERE address = ? AND discord_id = ?',
                    (transactions[0].tx_hash, wallet_address, discord_id)
                )
                conn.commit()

            # Process notifications outside DB transaction
            try:
                user = await self.fetch_user(int(discord_id))
                if user:
                    for tx in transactions:
                        if tx.tx_hash == last_tx[0]:
                            break
                        
                        embed = discord.Embed(
                            title="🔔 New Transaction",
                            description=f"New transaction detected for wallet: `{wallet_address}`",
                            color=discord.Color.blue()
                        )
                        embed.add_field(name="Transaction Hash", value=f"`{tx.tx_hash}`", inline=False)
                        embed.add_field(name="View Transaction", 
                                      value=f"[View on Cardanoscan](https://cardanoscan.io/transaction/{tx.tx_hash})", 
                                      inline=False)
                        await user.send(embed=embed)
            except Exception as e:
                logger.error(f"Error notifying user {discord_id} about transaction: {e}")
                
        except sqlite3.Error as e:
            logger.error(f"Database error checking transactions for wallet {wallet_address}: {e}")
        except Exception as e:
            logger.error(f"Error checking transactions for wallet {wallet_address}: {e}")

    async def check_yummi_balance(self, wallet_address):
        """Check YUMMI token balance"""
        try:
            if not self.blockfrost_client:
                return False, "Bot API connection is not available. Please try again later."

            # Get all asset balances for the wallet
            try:
                balances = await self.blockfrost_client.address_assets(wallet_address)
            except Exception as e:
                if "invalid address" in str(e).lower():
                    return False, "Invalid Cardano wallet address. Please check the address and try again."
                elif "not found" in str(e).lower():
                    return False, "Wallet not found on the blockchain. Please check the address and try again."
                else:
                    logger.error(f"Error checking balances: {e}")
                    return False, "Failed to check wallet balances. Please try again later."

            # Find YUMMI token balance
            yummi_balance = 0
            for asset in balances:
                if asset.unit == YUMMI_POLICY_ID:
                    yummi_balance = int(asset.quantity)
                    break

            return True, yummi_balance

        except Exception as e:
            logger.error(f"Error checking YUMMI balance: {e}")
            return False, "Failed to check YUMMI balance. Please try again later."

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            if self.blockfrost_client:
                return
                
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_API_KEY,
                base_url=BLOCKFROST_BASE_URL
            )
            
            # Test connection with retry
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    await self.blockfrost_client.health()
                    logger.info("Connected to Blockfrost API")
                    
                    # Start background task only after successful connection
                    if not self.check_wallets.is_running():
                        self.bg_task = self.loop.create_task(self.check_wallets())
                        logger.info("Started wallet monitoring task")
                    return
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Blockfrost connection attempt {attempt + 1} failed: {e}")
                        await asyncio.sleep(retry_delay * (attempt + 1))
                    else:
                        raise
            
        except Exception as e:
            logger.error(f"Blockfrost initialization failed: {e}")
            self.blockfrost_client = None
            
            # Don't start background task if initialization fails
            if hasattr(self, 'bg_task') and self.bg_task.is_running():
                self.bg_task.cancel()

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
