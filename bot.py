import os
import logging
import asyncio
import sqlite3
from datetime import datetime, timedelta
import discord
from discord import app_commands
from discord.ext import tasks, commands
from blockfrost import BlockFrostApi, ApiError
import re
from config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Discord intents
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.messages = True

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
                    last_checked TIMESTAMP,
                    last_tx_hash TEXT,
                    last_yummi_check TIMESTAMP
                )
            ''')
            conn.commit()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

class WalletModal(discord.ui.Modal, title='Add Wallet'):
    def __init__(self, bot_instance):
        super().__init__()
        self.bot = bot_instance
        self.wallet = discord.ui.TextInput(
            label='Wallet Address',
            placeholder='Enter your Cardano wallet address',
            required=True
        )
        self.add_item(self.wallet)

    async def on_submit(self, interaction: discord.Interaction):
        wallet_address = self.wallet.value.strip()
        
        # Basic validation
        if not re.match(r'^addr1[a-zA-Z0-9]{98}$', wallet_address):
            await interaction.response.send_message("Invalid wallet address format. Please provide a valid Cardano address.")
            return

        try:
            # Check if wallet exists on blockchain
            await self.bot.blockfrost_client.address(wallet_address)
            
            # Check YUMMI balance
            has_tokens, message = await self.bot.check_yummi_balance(wallet_address)
            if not has_tokens:
                await interaction.response.send_message(f"Insufficient YUMMI tokens: {message}", ephemeral=True)
                return

            # Add wallet to database
            if self.bot.add_wallet(wallet_address, str(interaction.user.id)):
                embed = discord.Embed(
                    title="Wallet Added Successfully",
                    description=f"Now monitoring wallet:\n`{wallet_address}`\n\n{message}",
                    color=discord.Color.green()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message("This wallet is already being monitored", ephemeral=True)

        except Exception as e:
            logger.error(f"Error adding wallet: {e}")
            await interaction.response.send_message("Error adding wallet. Please try again later.", ephemeral=True)

class WalletBud(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.blockfrost_client = None
        self.processing_wallets = False
        self.monitoring_paused = False
        self.wallet_check_task = tasks.loop(minutes=TRANSACTION_CHECK_INTERVAL)(self.wallet_check)
        
        # Initialize database
        if not init_db():
            raise RuntimeError("Failed to initialize database")

    async def setup_hook(self):
        """Initialize the bot's command tree and sync commands"""
        try:
            # Initialize Blockfrost
            await self.init_blockfrost()
            
            # Register commands
            self.tree.command(name="addwallet", description="Add a Cardano wallet for tracking")(self.add_wallet_command)
            self.tree.command(name="removewallet", description="Remove a wallet from monitoring")(self.remove_wallet)
            self.tree.command(name="listwallets", description="List all monitored wallets")(self.list_wallets)
            self.tree.command(name="health", description="Check bot health status")(self.health_check)
            self.tree.command(name="help", description="Show available commands")(self.help_command)
            self.tree.command(name="togglemonitor", description="Toggle wallet monitoring")(self.toggle_monitoring)
            
            # Start wallet checking task
            self.wallet_check_task.start()
            
            # Sync commands
            await self.tree.sync()
            logger.info("Commands synced successfully")
            
        except Exception as e:
            logger.error(f"Error in setup: {e}")
            raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client"""
        try:
            if self.blockfrost_client:
                return
                
            self.blockfrost_client = BlockFrostApi(
                project_id=BLOCKFROST_API_KEY,
                base_url=BLOCKFROST_BASE_URL
            )
            await self.blockfrost_client.health()
            logger.info("Connected to Blockfrost API")
        except Exception as e:
            logger.error(f"Blockfrost initialization failed: {e}")
            self.blockfrost_client = None
            raise

    async def wallet_check(self):
        """Check all active wallets for new transactions"""
        if not self.is_ready() or not self.blockfrost_client:
            return

        if self.monitoring_paused:
            logger.info("Monitoring is paused")
            return

        if self.processing_wallets:
            logger.info("Already processing wallets")
            return

        try:
            self.processing_wallets = True
            wallets = self.get_all_active_wallets()
            
            if not wallets:
                return

            logger.info(f"Checking {len(wallets)} wallets")
            
            for wallet_address, discord_id in wallets:
                try:
                    # Check YUMMI balance if needed
                    last_check = self.get_last_yummi_check(wallet_address)
                    if not last_check or \
                    datetime.utcnow() - last_check > timedelta(hours=YUMMI_CHECK_INTERVAL):
                        
                        has_tokens, message = await self.check_yummi_balance(wallet_address)
                        self.update_last_yummi_check(wallet_address)
                        
                        if not has_tokens:
                            self.update_wallet_status(wallet_address, False)
                            try:
                                user = await self.fetch_user(int(discord_id))
                                if user:
                                    embed = discord.Embed(
                                        title="Wallet Deactivated",
                                        description=message,
                                        color=discord.Color.red()
                                    )
                                    await user.send(embed=embed)
                            except Exception as e:
                                logger.error(f"Error notifying user {discord_id}: {e}")
                            continue

                    # Check transactions
                    await self.check_wallet_transactions(wallet_address, discord_id)
                    
                except Exception as e:
                    logger.error(f"Error processing wallet {wallet_address}: {e}")
                
                await asyncio.sleep(WALLET_CHECK_DELAY)

        except Exception as e:
            logger.error(f"Error in wallet check task: {e}")
        finally:
            self.processing_wallets = False

    async def before_wallet_check(self):
        """Wait until bot is ready before starting the task"""
        await self.wait_until_ready()
        logger.info("Starting wallet check task")

    async def close(self):
        """Cleanup when bot is shutting down"""
        logger.info("Bot is shutting down...")
        if hasattr(self, 'wallet_check_task') and self.wallet_check_task.is_running():
            self.wallet_check_task.cancel()
        await super().close()

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check transactions for a wallet"""
        try:
            last_tx = self.get_last_tx_hash(wallet_address)
            
            transactions = []
            async for tx in self.blockfrost_client.address_transactions_all(wallet_address):
                transactions.append(tx)
                if len(transactions) >= MAX_TX_HISTORY:
                    break

            if not transactions:
                return

            # Find new transactions
            new_txs = []
            for tx in transactions:
                if tx.tx_hash == last_tx:
                    break
                new_txs.append(tx)

            if new_txs:
                self.update_last_checked(wallet_address, new_txs[0].tx_hash)
                
                # Notify user
                try:
                    user = await self.fetch_user(int(discord_id))
                    if user:
                        for tx in reversed(new_txs):
                            embed = discord.Embed(
                                title="New Transaction",
                                description=f"Transaction Hash:\n`{tx.tx_hash}`\n\n[View on Cardanoscan](https://cardanoscan.io/transaction/{tx.tx_hash})",
                                color=discord.Color.blue()
                            )
                            await user.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error notifying user {discord_id}: {e}")

        except Exception as e:
            logger.error(f"Error checking transactions for {wallet_address}: {e}")

    async def check_yummi_balance(self, wallet_address):
        """Check YUMMI token balance"""
        try:
            utxos = await self.blockfrost_client.address_utxos(wallet_address)
            
            yummi_amount = 0
            for utxo in utxos:
                for amount in utxo.amount:
                    if hasattr(amount, 'unit') and amount.unit.startswith(YUMMI_POLICY_ID):
                        yummi_amount += int(amount.quantity)

            if yummi_amount >= REQUIRED_BUD_TOKENS:
                return True, f"Wallet has {yummi_amount:,} YUMMI tokens"
            else:
                return False, f"Insufficient YUMMI tokens: {yummi_amount:,}/{REQUIRED_BUD_TOKENS:,} required"

        except Exception as e:
            logger.error(f"Error checking YUMMI balance: {e}")
            return False, str(e)

    # Database operations
    def get_all_active_wallets(self):
        """Get all active wallets"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT address, discord_id FROM wallets WHERE is_active = TRUE')
            return cursor.fetchall()

    def add_wallet(self, wallet_address, discord_id):
        """Add a new wallet"""
        try:
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT INTO wallets (address, discord_id, last_checked, last_yummi_check) VALUES (?, ?, ?, ?)',
                    (wallet_address, discord_id, datetime.utcnow(), datetime.utcnow())
                )
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False

    def update_last_checked(self, wallet_address, tx_hash=None):
        """Update last checked time"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            if tx_hash:
                cursor.execute(
                    'UPDATE wallets SET last_checked = ?, last_tx_hash = ? WHERE address = ?',
                    (datetime.utcnow(), tx_hash, wallet_address)
                )
            else:
                cursor.execute(
                    'UPDATE wallets SET last_checked = ? WHERE address = ?',
                    (datetime.utcnow(), wallet_address)
                )
            conn.commit()

    def update_last_yummi_check(self, wallet_address):
        """Update last YUMMI check time"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE wallets SET last_yummi_check = ? WHERE address = ?',
                (datetime.utcnow(), wallet_address)
            )
            conn.commit()

    def get_last_yummi_check(self, wallet_address):
        """Get last YUMMI check time"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT last_yummi_check FROM wallets WHERE address = ?',
                (wallet_address,)
            )
            result = cursor.fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
            return None

    def get_last_tx_hash(self, wallet_address):
        """Get last transaction hash"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT last_tx_hash FROM wallets WHERE address = ?',
                (wallet_address,)
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def update_wallet_status(self, wallet_address, is_active):
        """Update wallet status"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE wallets SET is_active = ? WHERE address = ?',
                (is_active, wallet_address)
            )
            conn.commit()

    def get_user_wallets(self, discord_id):
        """Get user's wallets"""
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT address, is_active FROM wallets WHERE discord_id = ?',
                (discord_id,)
            )
            return cursor.fetchall()

    # Discord Commands
    async def add_wallet_command(self, interaction: discord.Interaction):
        """Add wallet command"""
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message("Please use this command in DMs!", ephemeral=True)
            return
        await interaction.response.send_modal(WalletModal(self))

    async def remove_wallet(self, interaction: discord.Interaction, wallet_address: str):
        """Remove wallet command"""
        try:
            with sqlite3.connect(DATABASE_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'DELETE FROM wallets WHERE discord_id = ? AND address = ?',
                    (str(interaction.user.id), wallet_address)
                )
                conn.commit()
                if cursor.rowcount > 0:
                    await interaction.response.send_message("Wallet removed successfully", ephemeral=True)
                else:
                    await interaction.response.send_message("Wallet not found", ephemeral=True)
        except Exception as e:
            logger.error(f"Error removing wallet: {e}")
            await interaction.response.send_message("Error removing wallet", ephemeral=True)

    async def list_wallets(self, interaction: discord.Interaction):
        """List wallets command"""
        wallets = self.get_user_wallets(str(interaction.user.id))
        
        if not wallets:
            await interaction.response.send_message("You have no monitored wallets", ephemeral=True)
            return

        embed = discord.Embed(
            title="Your Monitored Wallets",
            color=discord.Color.blue()
        )
        
        for address, is_active in wallets:
            status = "Active" if is_active else "Inactive"
            embed.add_field(
                name=f"Wallet ({status})",
                value=f"`{address}`",
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def health_check(self, interaction: discord.Interaction):
        """Health check command"""
        try:
            # Check Blockfrost
            blockfrost_status = "Connected"
            try:
                await self.blockfrost_client.health()
            except Exception as e:
                blockfrost_status = f"Error: {str(e)}"

            # Check database
            db_status = "Connected"
            try:
                with sqlite3.connect(DATABASE_NAME) as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM wallets')
                    wallet_count = cursor.fetchone()[0]
            except Exception as e:
                db_status = f"Error: {str(e)}"
                wallet_count = 0

            embed = discord.Embed(
                title="Bot Health Status",
                color=discord.Color.blue()
            )
            embed.add_field(name="Bot Status", value="Online", inline=False)
            embed.add_field(name="Blockfrost API", value=blockfrost_status, inline=False)
            embed.add_field(name="Database", value=f"{db_status}\nMonitored Wallets: {wallet_count}", inline=False)
            embed.add_field(name="Monitoring", value="Paused" if self.monitoring_paused else "Active", inline=False)

            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Error in health check: {e}")
            await interaction.response.send_message("Error checking health status", ephemeral=True)

    async def toggle_monitoring(self, interaction: discord.Interaction):
        """Toggle monitoring command"""
        self.monitoring_paused = not self.monitoring_paused
        status = "paused" if self.monitoring_paused else "resumed"
        await interaction.response.send_message(f"Monitoring {status}", ephemeral=True)

    async def help_command(self, interaction: discord.Interaction):
        """Help command"""
        embed = discord.Embed(
            title="WalletBud Commands",
            description="Here are all available commands:",
            color=discord.Color.blue()
        )
        
        commands = {
            "/addwallet": "Add a wallet to monitor (DM only)",
            "/removewallet": "Remove a wallet from monitoring",
            "/listwallets": "Show your monitored wallets",
            "/health": "Check bot status",
            "/togglemonitor": "Pause/resume monitoring",
            "/help": "Show this help message"
        }
        
        for cmd, desc in commands.items():
            embed.add_field(name=cmd, value=desc, inline=False)
            
        embed.set_footer(text="Note: The bot checks wallets every 5 minutes and YUMMI balance every 6 hours")
        await interaction.response.send_message(embed=embed, ephemeral=True)

if __name__ == "__main__":
    try:
        logger.info("Starting WalletBud bot...")
        bot = WalletBud()
        bot.run(DISCORD_TOKEN, log_handler=None)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
