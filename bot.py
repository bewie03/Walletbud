import os
import logging
import asyncio
import sqlite3
from datetime import datetime, timedelta
import discord
from discord import app_commands
from discord.ext import tasks, commands
from blockfrost import BlockFrostApi, ApiError
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Discord client
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.messages = True

# Initialize database
DB_FILE = "wallets.db"

def init_db():
    """Initialize SQLite database"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
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
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

# Initialize database
try:
    init_db()
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")
    exit(1)

# Initialize Blockfrost client
blockfrost_client = None

async def init_blockfrost():
    """Initialize Blockfrost API client"""
    try:
        global blockfrost_client
        blockfrost_key = os.getenv('BLOCKFROST_API_KEY')
        if not blockfrost_key:
            logger.error("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")
            exit(1)
        blockfrost_client = BlockFrostApi(
            project_id=blockfrost_key,
            base_url="https://cardano-mainnet.blockfrost.io/api/v0"
        )
        logger.info("Successfully connected to Blockfrost API")
    except Exception as e:
        logger.error(f"Failed to initialize Blockfrost client: {e}")
        raise

# Bot configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN:
    logger.error("No Discord token found! Make sure DISCORD_TOKEN is set in .env")
    exit(1)

YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID')
REQUIRED_BUD_TOKENS = int(os.getenv('REQUIRED_BUD_TOKENS', '20000'))
TRANSACTION_CHECK_INTERVAL = int(os.getenv('TRANSACTION_CHECK_INTERVAL', '5'))  # minutes
YUMMI_CHECK_INTERVAL = int(os.getenv('YUMMI_CHECK_INTERVAL', '6'))  # hours
MAX_TX_HISTORY = int(os.getenv('MAX_TX_HISTORY', '10'))  # Maximum number of transactions to check per wallet

class WalletBud(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.processing_wallets = False
        self.monitoring_paused = False
        self.tree = app_commands.CommandTree(self)
        
    async def setup_hook(self):
        """Setup the bot when it starts"""
        try:
            await init_blockfrost()  # Initialize Blockfrost first
            self.check_wallets.start()
        except Exception as e:
            logger.error(f"Failed to initialize bot: {e}")
            await self.close()
            exit(1)
        
    async def close(self):
        """Cleanup when bot is shutting down"""
        logger.info("Bot is shutting down...")
        if self.check_wallets.is_running():
            self.check_wallets.cancel()
        await super().close()
        
    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f"Logged in as {self.user.name}")
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} command(s)")
            # Set bot status
            await self.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="YUMMI wallets | /help"
                )
            )
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    def get_all_active_wallets(self):
        """Get all active wallets"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT address, discord_id FROM wallets WHERE is_active = TRUE'
            )
            return cursor.fetchall()

    def add_wallet(self, wallet_address, discord_id):
        """Add a new wallet to the database"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
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
        """Update the last checked time and optionally the last transaction hash"""
        with sqlite3.connect(DB_FILE) as conn:
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
        """Update the last YUMMI balance check time"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE wallets SET last_yummi_check = ? WHERE address = ?',
                (datetime.utcnow(), wallet_address)
            )
            conn.commit()

    def get_last_yummi_check(self, wallet_address):
        """Get the last YUMMI balance check time"""
        with sqlite3.connect(DB_FILE) as conn:
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
        """Get the last seen transaction hash for a wallet"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT last_tx_hash FROM wallets WHERE address = ?',
                (wallet_address,)
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def update_wallet_status(self, wallet_address, is_active):
        """Update the status of a wallet"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE wallets SET is_active = ? WHERE address = ?',
                (is_active, wallet_address)
            )
            conn.commit()

    def is_wallet_active(self, wallet_address):
        """Check if a wallet is active"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT is_active FROM wallets WHERE address = ?',
                (wallet_address,)
            )
            result = cursor.fetchone()
            return bool(result[0]) if result else False

    def get_user_wallets(self, discord_id):
        """Get all wallets for a user"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT address FROM wallets WHERE discord_id = ?',
                (discord_id,)
            )
            return [row[0] for row in cursor.fetchall()]

    def remove_wallet(self, discord_id, wallet_address):
        """Remove a wallet from a user"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'DELETE FROM wallets WHERE discord_id = ? AND address = ?',
                    (discord_id, wallet_address)
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error removing wallet: {e}")
            return False

    async def check_wallets(self):
        """Check all active wallets for new transactions"""
        if self.processing_wallets:
            logger.info("Still processing previous wallet check, skipping...")
            return

        if self.monitoring_paused:  # Check if monitoring is paused
            logger.info("Wallet monitoring is paused, skipping check...")
            return

        try:
            self.processing_wallets = True
            logger.info("Running wallet check task...")

            # Get all active wallets
            active_wallets = self.get_all_active_wallets()
            logger.info(f"Found {len(active_wallets)} active wallets")

            # Process wallets in smaller batches to avoid rate limits
            batch_size = 20
            for i in range(0, len(active_wallets), batch_size):
                wallet_batch = active_wallets[i:i + batch_size]

                for wallet_address, discord_id in wallet_batch:
                    if not self.is_wallet_active(wallet_address):
                        continue

                    # Check YUMMI balance if needed (every 6 hours)
                    last_yummi_check = self.get_last_yummi_check(wallet_address)
                    if not last_yummi_check or (datetime.utcnow() - last_yummi_check).total_seconds() > YUMMI_CHECK_INTERVAL * 3600:
                        try:
                            has_balance, message = await check_yummi_balance(wallet_address)
                            self.update_last_yummi_check(wallet_address)

                            if not has_balance:
                                logger.info(f"Deactivating wallet {wallet_address}: {message}")
                                self.update_wallet_status(wallet_address, False)
                                try:
                                    user = await self.fetch_user(int(discord_id))
                                    if user:
                                        embed = discord.Embed(
                                            title="❌ Wallet Deactivated",
                                            description=message,
                                            color=discord.Color.red()
                                        )
                                        await user.send(embed=embed)
                                except Exception as e:
                                    logger.error(f"Error notifying user {discord_id}: {str(e)}")
                                continue

                        except blockfrost.ApiError as e:
                            logger.error(f"Error checking YUMMI balance: {str(e)}")
                            if e.status_code == 429:  # Rate limit
                                await asyncio.sleep(1)
                                continue

                    # Check transactions
                    await self.check_wallet_transactions(wallet_address, discord_id)
                    await asyncio.sleep(0.1)  # Small delay between wallets

                # Add a delay between batches to avoid rate limits
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in check_wallets task: {str(e)}")

        finally:
            self.processing_wallets = False

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check transactions for a single wallet"""
        try:
            try:
                # Get last seen transaction hash
                last_tx_hash = self.get_last_tx_hash(wallet_address)

                # Get transactions - this returns an iterator
                transactions = []
                async for tx in blockfrost_client.address_transactions_all(wallet_address):
                    transactions.append(tx)
                    if len(transactions) >= MAX_TX_HISTORY:
                        break

                logger.info(f"Found {len(transactions)} recent transactions for wallet {wallet_address}")

                # Find the index of our last seen transaction
                start_index = 0
                if last_tx_hash:
                    for i, tx in enumerate(transactions):
                        if tx.tx_hash == last_tx_hash:
                            start_index = i + 1
                            break

                # Process only new transactions
                new_transactions = transactions[start_index:start_index + MAX_TX_HISTORY]
                if new_transactions:
                    logger.info(f"Processing {len(new_transactions)} new transactions")

                    for tx in new_transactions:
                        try:
                            # Get transaction UTXOs
                            tx_utxos = await blockfrost_client.transaction_utxos(tx.tx_hash)

                            # Process inputs and outputs
                            amount_in = 0
                            amount_out = 0
                            assets_in = []
                            assets_out = []

                            # Check inputs
                            for input in tx_utxos.inputs:
                                if input.address == wallet_address:
                                    amount_in += int(input.amount[0].quantity)
                                    if len(input.amount) > 1:  # Has tokens
                                        for asset in input.amount[1:]:
                                            assets_in.append(f"{int(asset.quantity)} {asset.unit}")

                            # Check outputs
                            for output in tx_utxos.outputs:
                                if output.address == wallet_address:
                                    amount_out += int(output.amount[0].quantity)
                                    if len(output.amount) > 1:  # Has tokens
                                        for asset in output.amount[1:]:
                                            assets_out.append(f"{int(asset.quantity)} {asset.unit}")

                            # Only send notification if there were actual transfers
                            if amount_in > 0 or amount_out > 0 or assets_in or assets_out:
                                try:
                                    user = await self.fetch_user(int(discord_id))
                                    if user:
                                        embed = discord.Embed(
                                            title="🔔 New Transaction Detected",
                                            description=f"Transaction: [{tx.tx_hash}](https://cardanoscan.io/transaction/{tx.tx_hash})",
                                            color=discord.Color.blue(),
                                            timestamp=datetime.fromtimestamp(tx_utxos.block_time)
                                        )

                                        # Add ADA amounts
                                        if amount_in > 0:
                                            embed.add_field(
                                                name="ADA Sent",
                                                value=f"{amount_in / 1000000:.6f} ADA",
                                                inline=True
                                            )
                                        if amount_out > 0:
                                            embed.add_field(
                                                name="ADA Received",
                                                value=f"{amount_out / 1000000:.6f} ADA",
                                                inline=True
                                            )

                                        # Add asset transfers
                                        if assets_in:
                                            embed.add_field(
                                                name="Assets Sent",
                                                value="\n".join(assets_in),
                                                inline=False
                                            )
                                        if assets_out:
                                            embed.add_field(
                                                name="Assets Received",
                                                value="\n".join(assets_out),
                                                inline=False
                                            )

                                        await user.send(embed=embed)

                                except Exception as e:
                                    logger.error(f"Error sending transaction notification: {str(e)}")

                            # Update last transaction hash after processing
                            self.update_last_checked(wallet_address, tx.tx_hash)

                        except blockfrost.ApiError as e:
                            logger.error(f"Error getting transaction UTXOs: {str(e)}")
                            if e.status_code == 429:  # Rate limit
                                await asyncio.sleep(1)  # Wait a bit before next request
                                continue

            except blockfrost.ApiError as e:
                logger.error(f"Error fetching transactions: {str(e)}")
                if e.status_code == 429:  # Rate limit
                    return  # Skip this check, will try again next interval

        except Exception as e:
            logger.error(f"Error in check_wallet_transactions: {str(e)}")

    @check_wallets.before_loop
    async def before_check_wallets(self):
        """Wait until the bot is ready before starting the task"""
        await self.wait_until_ready()
        logger.info("Bot is ready, wallet check task can start")

    @app_commands.command(name="addwallet", description="Add a Cardano wallet for tracking (requires 20,000 YUMMI tokens)")
    async def add_wallet(self, interaction: discord.Interaction, wallet_address: str):
        """Add a wallet to monitor"""
        try:
            # Validate the wallet address
            if not re.match(r'^addr1[a-zA-Z0-9]{98}$', wallet_address):
                embed = discord.Embed(
                    title="❌ Invalid Wallet Address",
                    description="Please provide a valid Cardano wallet address starting with 'addr1'",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                return

            # Check if wallet already exists
            existing_wallets = self.get_user_wallets(str(interaction.user.id))
            if wallet_address in existing_wallets:
                embed = discord.Embed(
                    title="❌ Wallet Already Added",
                    description="This wallet is already being monitored",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                return

            # Check YUMMI balance before adding
            has_balance, message = await check_yummi_balance(wallet_address)
            if not has_balance:
                embed = discord.Embed(
                    title="❌ Insufficient YUMMI Balance",
                    description=message,
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                return

            # Add wallet to database
            if self.add_wallet(wallet_address, str(interaction.user.id)):
                embed = discord.Embed(
                    title="✅ Wallet Added Successfully",
                    description=f"Now monitoring wallet:\n`{wallet_address}`",
                    color=discord.Color.green()
                )
                embed.add_field(
                    name="Monitoring Details",
                    value=f"• Transaction checks every {TRANSACTION_CHECK_INTERVAL} minutes\n"
                          f"• YUMMI balance checks every {YUMMI_CHECK_INTERVAL} hours\n"
                          f"• You'll be notified of any transactions or changes",
                    inline=False
                )
                await interaction.response.send_message(embed=embed)
            else:
                embed = discord.Embed(
                    title="❌ Error",
                    description="Failed to add wallet. Please try again later.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)

        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while adding the wallet",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    @app_commands.command(name="remove_wallet", description="Remove a wallet from tracking")
    async def remove_wallet(self, interaction: discord.Interaction, wallet_address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists and belongs to user
            user_wallets = self.get_user_wallets(str(interaction.user.id))
            if wallet_address not in user_wallets:
                embed = discord.Embed(
                    title="❌ Wallet Not Found",
                    description="This wallet is not in your monitoring list",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                return

            # Remove wallet from database
            if self.remove_wallet(str(interaction.user.id), wallet_address):
                embed = discord.Embed(
                    title="✅ Wallet Removed Successfully",
                    description=f"Stopped monitoring wallet:\n`{wallet_address}`",
                    color=discord.Color.green()
                )
                await interaction.response.send_message(embed=embed)
            else:
                embed = discord.Embed(
                    title="❌ Error",
                    description="Failed to remove wallet. Please try again later.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)

        except Exception as e:
            logger.error(f"Error removing wallet: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while removing the wallet",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    @app_commands.command(name="list_wallets", description="List your registered wallets")
    async def list_wallets(self, interaction: discord.Interaction):
        """List all monitored wallets"""
        try:
            wallets = self.get_user_wallets(str(interaction.user.id))
            if not wallets:
                embed = discord.Embed(
                    title="📝 Your Monitored Wallets",
                    description="You don't have any wallets being monitored",
                    color=discord.Color.blue()
                )
                await interaction.response.send_message(embed=embed)
                return

            embed = discord.Embed(
                title="📝 Your Monitored Wallets",
                color=discord.Color.blue()
            )
            for i, wallet in enumerate(wallets, 1):
                embed.add_field(
                    name=f"Wallet {i}",
                    value=f"`{wallet}`",
                    inline=False
                )
            await interaction.response.send_message(embed=embed)

        except Exception as e:
            logger.error(f"Error listing wallets: {str(e)}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while listing your wallets",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)

    @app_commands.command(name="health", description="Check the bot's health status")
    async def health_check(self, interaction: discord.Interaction):
        """Check the health status of the bot and its connections"""
        try:
            # Check database connection
            db_status = "✅ Connected"
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM wallets')
                    wallet_count = cursor.fetchone()[0]
            except Exception as e:
                db_status = f"❌ Error: {str(e)}"

            # Check Blockfrost API
            api_status = "✅ Connected"
            try:
                test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
                await blockfrost_client.address(test_address)
            except Exception as e:
                api_status = f"❌ Error: {str(e)}"

            # Create status embed
            embed = discord.Embed(
                title="🏥 Bot Health Status",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="Bot Status",
                value="✅ Online" if not self.monitoring_paused else "⏸️ Monitoring Paused",
                inline=False
            )

            embed.add_field(
                name="Database Status",
                value=f"{db_status}\nTotal Wallets: {wallet_count if 'wallet_count' in locals() else 'N/A'}",
                inline=False
            )

            embed.add_field(
                name="Blockfrost API Status",
                value=api_status,
                inline=False
            )

            embed.add_field(
                name="Monitoring Settings",
                value=f"Transaction Check: Every {TRANSACTION_CHECK_INTERVAL} minutes\nYUMMI Check: Every {YUMMI_CHECK_INTERVAL} hours",
                inline=False
            )

            await interaction.response.send_message(embed=embed)

        except Exception as e:
            logger.error(f"Error in health check: {str(e)}")
            await interaction.response.send_message(
                "Failed to check bot health. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="togglemonitor", description="Pause or resume wallet monitoring")
    @app_commands.default_permissions(administrator=True)
    async def toggle_monitoring(self, interaction: discord.Interaction):
        """Toggle wallet monitoring on/off"""
        try:
            self.monitoring_paused = not self.monitoring_paused
            status = "paused" if self.monitoring_paused else "resumed"

            embed = discord.Embed(
                title=f"🔄 Monitoring {status.capitalize()}",
                description=f"Wallet monitoring has been {status}.",
                color=discord.Color.orange() if self.monitoring_paused else discord.Color.green(),
                timestamp=datetime.utcnow()
            )

            if self.monitoring_paused:
                embed.add_field(
                    name="Note",
                    value="No transaction notifications will be sent while monitoring is paused.",
                    inline=False
                )

            await interaction.response.send_message(embed=embed)
            logger.info(f"Wallet monitoring {status} by {interaction.user.name} ({interaction.user.id})")

        except Exception as e:
            logger.error(f"Error toggling monitoring: {str(e)}")
            await interaction.response.send_message(
                "Failed to toggle monitoring. Please try again later.",
                ephemeral=True
            )

    @tasks.loop(minutes=TRANSACTION_CHECK_INTERVAL)
    async def check_wallets(self):
        """Check all active wallets for new transactions"""
        if self.processing_wallets:
            logger.info("Still processing previous wallet check, skipping...")
            return

        if self.monitoring_paused:  # Check if monitoring is paused
            logger.info("Wallet monitoring is paused, skipping check...")
            return

        try:
            self.processing_wallets = True
            logger.info("Running wallet check task...")

            # Get all active wallets
            active_wallets = self.get_all_active_wallets()
            logger.info(f"Found {len(active_wallets)} active wallets")

            # Process wallets in smaller batches to avoid rate limits
            batch_size = 20
            for i in range(0, len(active_wallets), batch_size):
                wallet_batch = active_wallets[i:i + batch_size]

                for wallet_address, discord_id in wallet_batch:
                    if not self.is_wallet_active(wallet_address):
                        continue

                    # Check YUMMI balance if needed (every 6 hours)
                    last_yummi_check = self.get_last_yummi_check(wallet_address)
                    if not last_yummi_check or (datetime.utcnow() - last_yummi_check).total_seconds() > YUMMI_CHECK_INTERVAL * 3600:
                        try:
                            has_balance, message = await check_yummi_balance(wallet_address)
                            self.update_last_yummi_check(wallet_address)

                            if not has_balance:
                                logger.info(f"Deactivating wallet {wallet_address}: {message}")
                                self.update_wallet_status(wallet_address, False)
                                try:
                                    user = await self.fetch_user(int(discord_id))
                                    if user:
                                        embed = discord.Embed(
                                            title="❌ Wallet Deactivated",
                                            description=message,
                                            color=discord.Color.red()
                                        )
                                        await user.send(embed=embed)
                                except Exception as e:
                                    logger.error(f"Error notifying user {discord_id}: {str(e)}")
                                continue

                        except blockfrost.ApiError as e:
                            logger.error(f"Error checking YUMMI balance: {str(e)}")
                            if e.status_code == 429:  # Rate limit
                                await asyncio.sleep(1)
                                continue

                    # Check transactions
                    await self.check_wallet_transactions(wallet_address, discord_id)
                    await asyncio.sleep(0.1)  # Small delay between wallets

                # Add a delay between batches to avoid rate limits
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in check_wallets task: {str(e)}")

        finally:
            self.processing_wallets = False

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check transactions for a single wallet"""
        try:
            try:
                # Get last seen transaction hash
                last_tx_hash = self.get_last_tx_hash(wallet_address)

                # Get transactions - this returns an iterator
                transactions = []
                async for tx in blockfrost_client.address_transactions_all(wallet_address):
                    transactions.append(tx)
                    if len(transactions) >= MAX_TX_HISTORY:
                        break

                logger.info(f"Found {len(transactions)} recent transactions for wallet {wallet_address}")

                # Find the index of our last seen transaction
                start_index = 0
                if last_tx_hash:
                    for i, tx in enumerate(transactions):
                        if tx.tx_hash == last_tx_hash:
                            start_index = i + 1
                            break

                # Process only new transactions
                new_transactions = transactions[start_index:start_index + MAX_TX_HISTORY]
                if new_transactions:
                    logger.info(f"Processing {len(new_transactions)} new transactions")

                    for tx in new_transactions:
                        try:
                            # Get transaction UTXOs
                            tx_utxos = await blockfrost_client.transaction_utxos(tx.tx_hash)

                            # Process inputs and outputs
                            amount_in = 0
                            amount_out = 0
                            assets_in = []
                            assets_out = []

                            # Check inputs
                            for input in tx_utxos.inputs:
                                if input.address == wallet_address:
                                    amount_in += int(input.amount[0].quantity)
                                    if len(input.amount) > 1:  # Has tokens
                                        for asset in input.amount[1:]:
                                            assets_in.append(f"{int(asset.quantity)} {asset.unit}")

                            # Check outputs
                            for output in tx_utxos.outputs:
                                if output.address == wallet_address:
                                    amount_out += int(output.amount[0].quantity)
                                    if len(output.amount) > 1:  # Has tokens
                                        for asset in output.amount[1:]:
                                            assets_out.append(f"{int(asset.quantity)} {asset.unit}")

                            # Only send notification if there were actual transfers
                            if amount_in > 0 or amount_out > 0 or assets_in or assets_out:
                                try:
                                    user = await self.fetch_user(int(discord_id))
                                    if user:
                                        embed = discord.Embed(
                                            title="🔔 New Transaction Detected",
                                            description=f"Transaction: [{tx.tx_hash}](https://cardanoscan.io/transaction/{tx.tx_hash})",
                                            color=discord.Color.blue(),
                                            timestamp=datetime.fromtimestamp(tx_utxos.block_time)
                                        )

                                        # Add ADA amounts
                                        if amount_in > 0:
                                            embed.add_field(
                                                name="ADA Sent",
                                                value=f"{amount_in / 1000000:.6f} ADA",
                                                inline=True
                                            )
                                        if amount_out > 0:
                                            embed.add_field(
                                                name="ADA Received",
                                                value=f"{amount_out / 1000000:.6f} ADA",
                                                inline=True
                                            )

                                        # Add asset transfers
                                        if assets_in:
                                            embed.add_field(
                                                name="Assets Sent",
                                                value="\n".join(assets_in),
                                                inline=False
                                            )
                                        if assets_out:
                                            embed.add_field(
                                                name="Assets Received",
                                                value="\n".join(assets_out),
                                                inline=False
                                            )

                                        await user.send(embed=embed)

                                except Exception as e:
                                    logger.error(f"Error sending transaction notification: {str(e)}")

                            # Update last transaction hash after processing
                            self.update_last_checked(wallet_address, tx.tx_hash)

                        except blockfrost.ApiError as e:
                            logger.error(f"Error getting transaction UTXOs: {str(e)}")
                            if e.status_code == 429:  # Rate limit
                                await asyncio.sleep(1)  # Wait a bit before next request
                                continue

            except blockfrost.ApiError as e:
                logger.error(f"Error fetching transactions: {str(e)}")
                if e.status_code == 429:  # Rate limit
                    return  # Skip this check, will try again next interval

        except Exception as e:
            logger.error(f"Error in check_wallet_transactions: {str(e)}")

    @check_wallets.before_loop
    async def before_check_wallets(self):
        """Wait until the bot is ready before starting the task"""
        await self.wait_until_ready()
        logger.info("Bot is ready, wallet check task can start")

    @app_commands.command(name="help", description="Show available commands")
    async def help_command(self, interaction: discord.Interaction):
        """Show help information about available commands"""
        try:
            embed = discord.Embed(
                title="🤖 WalletBud Commands",
                description="Here are all available commands:",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="/addwallet",
                value="Add a Cardano wallet for monitoring (requires 20,000 YUMMI tokens)",
                inline=False
            )
            
            embed.add_field(
                name="/removewallet",
                value="Remove a wallet from monitoring",
                inline=False
            )
            
            embed.add_field(
                name="/listwallets",
                value="List all your monitored wallets",
                inline=False
            )
            
            embed.add_field(
                name="/health",
                value="Check the bot's health status",
                inline=False
            )
            
            if interaction.user.guild_permissions.administrator:
                embed.add_field(
                    name="/togglemonitor",
                    value="[Admin] Pause or resume wallet monitoring",
                    inline=False
                )
                
            embed.set_footer(text="For more help, contact the bot administrator")
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in help command: {str(e)}")
            await interaction.response.send_message(
                "Failed to show help information. Please try again later.",
                ephemeral=True
            )

bot = WalletBud()

@bot.tree.command(name="addwallet", description="Add a Cardano wallet for tracking (requires 20,000 YUMMI tokens)")
async def add_wallet(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.DMChannel):
        await interaction.response.send_message("Please use this command in DMs!", ephemeral=True)
        return
    await interaction.response.send_modal(WalletModal())

class WalletModal(discord.ui.Modal, title='Add Wallet'):
    wallet = discord.ui.TextInput(
        label='Wallet Address',
        placeholder='Enter your Cardano wallet address',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        wallet_address = self.wallet.value.strip()

        if not wallet_address.startswith(('addr1', 'addr_test1')):
            await interaction.response.send_message("Invalid wallet address format. Please provide a valid Cardano address.")
            return

        has_balance, message = await check_yummi_balance(wallet_address)
        if not has_balance:
            embed = discord.Embed(
                title="❌ Wallet Check Failed",
                description=message,
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return

        try:
            if bot.add_wallet(wallet_address, str(interaction.user.id)):
                bot.update_wallet_status(wallet_address, True)
                embed = discord.Embed(
                    title="✅ Wallet Added Successfully!",
                    description="You will receive DM notifications for transactions.",
                    color=discord.Color.green()
                )
                embed.add_field(
                    name="Wallet",
                    value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
                    inline=False
                )
                await interaction.response.send_message(embed=embed)
            else:
                await interaction.response.send_message("This wallet is already registered.")
        except Exception as e:
            logger.error(f"Error adding wallet: {str(e)}")
            await interaction.response.send_message("An error occurred while adding the wallet. Please try again.")

async def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        logger.info(f"Checking YUMMI balance for wallet: {wallet_address}")
        
        # Get wallet UTXOs
        utxos = await blockfrost_client.address_utxos(wallet_address)
        if not utxos:
            logger.info(f"No UTXOs found for wallet: {wallet_address}")
            return False, "No UTXOs found in wallet"
            
        # Look for YUMMI tokens in UTXOs
        yummi_amount = 0
        for utxo in utxos:
            for amount in utxo.amount:
                if hasattr(amount, 'unit') and amount.unit.startswith(YUMMI_POLICY_ID):
                    yummi_amount += int(amount.quantity)
                    logger.info(f"Found {amount.quantity} YUMMI tokens in UTXO")
        
        logger.info(f"Total YUMMI balance for {wallet_address}: {yummi_amount}")
        
        if yummi_amount >= REQUIRED_BUD_TOKENS:
            logger.info(f"Wallet has sufficient YUMMI balance: {yummi_amount}")
            return True, f"Wallet has {yummi_amount:,} YUMMI tokens"
        else:
            logger.info(f"Insufficient YUMMI balance: {yummi_amount}/{REQUIRED_BUD_TOKENS}")
            return False, f"Insufficient YUMMI tokens: {yummi_amount:,}/{REQUIRED_BUD_TOKENS:,} required"
            
    except Exception as e:
        logger.error(f"Error checking YUMMI balance: {str(e)}")
        if isinstance(e, blockfrost.ApiError):
            if e.status_code == 400:
                return False, "Invalid wallet address format"
            elif e.status_code == 404:
                return False, "Wallet not found"
            elif e.status_code == 429:
                return False, "Rate limit exceeded, please try again later"
            else:
                return False, f"API Error: {str(e)}"
        return False, f"Error checking wallet: {str(e)}"

if __name__ == "__main__":
    try:
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        exit(1)
