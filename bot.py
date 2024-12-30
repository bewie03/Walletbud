import os
import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import sqlite3
from datetime import datetime, timedelta
import asyncio
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
    'dm_only': "This command can only be used in DMs for security."
}

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

def dm_only():
    """Check if command is being used in DMs"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(ERROR_MESSAGES['dm_only'], ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

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
        intents = discord.Intents.default()
        intents.dm_messages = True  # For DM notifications
        intents.guilds = True      # Required for slash commands
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents)
        
        # Initialize core components
        self.blockfrost_client = None
        self.processing_wallets = False
        self.monitoring_paused = False
        self._command_tree_synced = False
        
        # Remove default help command
        self.remove_command('help')

    async def setup_hook(self):
        """Initialize the bot's command tree and sync commands"""
        try:
            # Initialize database
            if not init_db():
                raise RuntimeError("Failed to initialize database")

            # Initialize Blockfrost API (non-blocking)
            try:
                await self.init_blockfrost()
            except Exception as e:
                logger.error(f"Blockfrost initialization failed: {e}")
                logger.warning("Bot will start without Blockfrost...")
            
            # Only sync commands once
            if not self._command_tree_synced:
                await self.tree.sync()
                self._command_tree_synced = True
                logger.info("Commands synced successfully")
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

    async def on_command_error(self, ctx, error):
        """Global error handler for commands"""
        try:
            if isinstance(error, commands.NoPrivateMessage):
                await ctx.send(ERROR_MESSAGES['dm_only'])
            elif isinstance(error, commands.MissingPermissions):
                await ctx.send("You don't have permission to use this command.")
            elif isinstance(error, commands.CommandOnCooldown):
                await ctx.send(f"Please wait {error.retry_after:.1f}s before using this command again.")
            else:
                logger.error(f"Command error: {error}")
                await ctx.send("An error occurred. Please try again later.")
        except Exception as e:
            logger.error(f"Error in error handler: {e}")

    async def on_error(self, event, *args, **kwargs):
        """Global error handler for events"""
        try:
            logger.error(f"Event error in {event}: {args} {kwargs}")
        except Exception as e:
            logger.error(f"Error in error handler: {e}")

    @app_commands.command(name="addwallet", description="Add a Cardano wallet for tracking")
    @dm_only()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def add_wallet_command(self, interaction: discord.Interaction):
        if not self.blockfrost_client:
            embed = discord.Embed(
                title="❌ Error",
                description="Wallet monitoring is currently unavailable. Please try again later.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        modal = WalletModal(self)
        await interaction.response.send_modal(modal)

    @app_commands.command(name="removewallet", description="Remove a wallet from monitoring")
    @dm_only()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def remove_wallet(self, interaction: discord.Interaction, wallet_address: str):
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT discord_id FROM wallets WHERE address = ?', (wallet_address,))
            existing = cursor.fetchone()
            
            if existing:
                if existing[0] == str(interaction.user.id):
                    cursor.execute('DELETE FROM wallets WHERE address = ?', (wallet_address,))
                    conn.commit()
                    embed = discord.Embed(
                        title="✅ Wallet Removed",
                        description=f"Your wallet `{wallet_address}` has been removed from monitoring.",
                        color=discord.Color.green()
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                else:
                    embed = discord.Embed(
                        title="❌ Error",
                        description="This wallet does not belong to you.",
                        color=discord.Color.red()
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                embed = discord.Embed(
                    title="❌ Error",
                    description="This wallet is not being monitored.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="listwallets", description="List all monitored wallets")
    @dm_only()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def list_wallets(self, interaction: discord.Interaction):
        with sqlite3.connect(DATABASE_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT address, is_active FROM wallets WHERE discord_id = ?', (str(interaction.user.id),))
            wallets = cursor.fetchall()
        
        if not wallets:
            embed = discord.Embed(
                title="No Wallets",
                description="You don't have any wallets being monitored.",
                color=discord.Color.blue()
            )
        else:
            embed = discord.Embed(
                title="Your Monitored Wallets",
                color=discord.Color.blue()
            )
            for wallet in wallets:
                status = "🟢 Active" if wallet[1] else "🔴 Inactive"
                embed.add_field(
                    name=f"Wallet ({status})",
                    value=f"`{wallet[0]}`",
                    inline=False
                )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="health", description="Check bot health status")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def health_check(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Bot Health Status",
            color=discord.Color.blue()
        )
        
        # Check Blockfrost
        blockfrost_status = "🟢 Connected" if self.blockfrost_client else "🔴 Disconnected"
        embed.add_field(
            name="Blockfrost API",
            value=blockfrost_status,
            inline=False
        )
        
        # Check wallet monitoring
        monitoring_status = "🔴 Paused" if self.monitoring_paused else "🟢 Active"
        if not self.blockfrost_client:
            monitoring_status = "🔴 Unavailable (No API Connection)"
            
        embed.add_field(
            name="Wallet Monitoring",
            value=monitoring_status,
            inline=False
        )
        
        # Add processing status
        processing_status = "⏳ Processing" if self.processing_wallets else "✅ Idle"
        embed.add_field(
            name="Processing Status",
            value=processing_status,
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="help", description="Show available commands")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def help_command(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="WalletBud Commands",
            description="Here are all available commands:",
            color=discord.Color.blue()
        )
        
        commands = {
            "/addwallet": "Add a Cardano wallet for monitoring (DM only)",
            "/removewallet": "Remove a wallet from monitoring (DM only)",
            "/listwallets": "View your monitored wallets (DM only)",
            "/health": "Check bot's health status",
            "/togglemonitor": "Pause/resume wallet monitoring",
            "/help": "Show this help message"
        }
        
        for cmd, desc in commands.items():
            embed.add_field(name=cmd, value=desc, inline=False)
        
        embed.set_footer(text="Note: The bot checks wallets every 5 minutes and YUMMI balance every 6 hours")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="togglemonitor", description="Toggle wallet monitoring")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def toggle_monitoring(self, interaction: discord.Interaction):
        if not self.blockfrost_client:
            embed = discord.Embed(
                title="❌ Error",
                description="Monitoring is unavailable due to API connection issues.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        self.monitoring_paused = not self.monitoring_paused
        status = "paused" if self.monitoring_paused else "resumed"
        
        embed = discord.Embed(
            title="✅ Monitor Status Updated",
            description=f"Wallet monitoring has been {status}.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def check_wallets(self):
        """Check all active wallets for new transactions"""
        while True:
            if not self.is_ready() or not self.blockfrost_client:
                await asyncio.sleep(WALLET_CHECK_DELAY)
                continue

            if self.monitoring_paused:
                logger.info("Monitoring is paused")
                await asyncio.sleep(WALLET_CHECK_DELAY)
                continue

            if self.processing_wallets:
                logger.info("Already processing wallets")
                await asyncio.sleep(WALLET_CHECK_DELAY)
                continue

            try:
                self.processing_wallets = True
                logger.info("Starting wallet check cycle")
                
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
                    await asyncio.sleep(WALLET_CHECK_DELAY)
                    continue

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
                await asyncio.sleep(WALLET_CHECK_DELAY)

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
