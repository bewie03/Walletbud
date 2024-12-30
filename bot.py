import os
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from database import Database
import blockfrost
from datetime import datetime, timedelta
from config import YUMMI_POLICY_ID, REQUIRED_BUD_TOKENS, TRANSACTION_CHECK_INTERVAL, MAX_TX_HISTORY
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Check if token is loaded
token = os.getenv('DISCORD_TOKEN')
if not token:
    logger.error("No Discord token found! Make sure DISCORD_TOKEN is set in .env")
    exit(1)

# Set up intents
intents = discord.Intents.all()  # Enable all intents
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.messages = True

class WalletBud(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.db = Database()

    async def setup_hook(self):
        # Force sync all commands
        try:
            logger.info("Attempting to sync commands...")
            commands = await self.tree.sync()
            logger.info(f"Successfully synced {len(commands)} commands")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    async def close(self):
        """Cleanup when bot is shutting down"""
        logger.info("Bot is shutting down...")
        if hasattr(self, 'db'):
            self.db.close()
            logger.info("Database connection closed")
        await super().close()

    async def on_ready(self):
        logger.info(f'{self.user} has connected to Discord!')
        # Start background tasks
        if not self.check_wallets.is_running():
            self.check_wallets.start()
            logger.info("Started wallet checking task")
        # Set presence
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="YUMMI wallets | DM me!"
            )
        )
        logger.info("Bot presence updated")

    @tasks.loop(minutes=TRANSACTION_CHECK_INTERVAL)
    async def check_wallets(self):
        """Check all active wallets for new transactions"""
        try:
            logger.info("Running wallet check task...")
            active_wallets = self.db.get_all_active_wallets()
            logger.info(f"Found {len(active_wallets)} active wallets")
            for wallet_address, discord_id in active_wallets:
                await self.check_wallet_transactions(wallet_address, discord_id)
        except Exception as e:
            logger.error(f"Error in check_wallets task: {e}")

    @check_wallets.before_loop
    async def before_check_wallets(self):
        """Wait until the bot is ready before starting the task"""
        await self.wait_until_ready()
        logger.info("Bot is ready, wallet check task can start")

    async def check_wallet_transactions(self, wallet_address, discord_id):
        """Check transactions for a single wallet"""
        try:
            # Check YUMMI balance
            has_balance, message = check_yummi_balance(wallet_address)
            if not has_balance:
                logger.info(f"Deactivating wallet {wallet_address}: {message}")
                self.db.update_wallet_status(wallet_address, False)
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
                return

            # Check transactions
            try:
                txs = blockfrost_client.address_transactions(
                    wallet_address,
                    count=MAX_TX_HISTORY
                )
                
                for tx in txs:
                    try:
                        tx_details = blockfrost_client.transaction(tx.tx_hash)
                        embed = discord.Embed(
                            title="🔔 New Transaction",
                            color=discord.Color.blue()
                        )
                        embed.add_field(
                            name="Transaction Hash",
                            value=f"[View on Cardanoscan](https://cardanoscan.io/transaction/{tx.tx_hash})",
                            inline=False
                        )
                        
                        user = await self.fetch_user(int(discord_id))
                        if user:
                            await user.send(embed=embed)
                            
                    except Exception as e:
                        logger.error(f"Error processing transaction {tx.tx_hash}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error checking transactions for {wallet_address}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing wallet {wallet_address}: {str(e)}")

# Constants
YUMMI_POLICY_ID = YUMMI_POLICY_ID
MIN_YUMMI_REQUIRED = REQUIRED_BUD_TOKENS

# Blockfrost setup
project_id = os.getenv('BLOCKFROST_API_KEY')
logger.info("Initializing Blockfrost client...")

if not project_id:
    logger.error("No Blockfrost API key found in environment variables")
    raise ValueError("BLOCKFROST_API_KEY environment variable is not set")

try:
    # Remove any whitespace from API key
    project_id = project_id.strip()
    
    # Initialize client
    blockfrost_client = blockfrost.BlockFrostApi(
        project_id=project_id
    )
    
    # Test the connection
    info = blockfrost_client.health()
    logger.info("Blockfrost connection successful")
        
except blockfrost.ApiError as e:
    logger.error(f"Blockfrost API Error: {str(e)}")
    if e.status_code == 403:
        logger.error("Invalid Blockfrost API key")
    elif e.status_code == 429:
        logger.error("Rate limit exceeded")
    raise
except Exception as e:
    logger.error(f"Failed to initialize Blockfrost client: {str(e)}")
    raise

def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        logger.info(f"Checking wallet {wallet_address} for YUMMI tokens")
        
        # Check if we have Blockfrost API key
        if not project_id:
            logger.error("No Blockfrost API key found")
            return False, "Configuration error: No Blockfrost API key"

        # First verify the wallet exists
        try:
            wallet = blockfrost_client.address(wallet_address)
            logger.info(f"Wallet verified: {wallet_address}")
        except blockfrost.ApiError as e:
            if e.status_code == 400:
                logger.error(f"Invalid wallet address format: {wallet_address}")
                return False, "Invalid wallet address format"
            elif e.status_code == 404:
                logger.error(f"Wallet not found: {wallet_address}")
                return False, "Wallet not found on the blockchain"
            elif e.status_code == 429:
                logger.error("Blockfrost rate limit exceeded")
                return False, "Service temporarily unavailable (rate limit)"
            else:
                logger.error(f"Blockfrost API error: {str(e)}")
                return False, "Error verifying wallet"

        # Get all assets in the wallet
        try:
            assets = blockfrost_client.address_assets(wallet_address)
            
            # Look for YUMMI token
            for asset in assets:
                if asset.unit.startswith(YUMMI_POLICY_ID):
                    yummi_amount = int(asset.quantity)
                    logger.info(f"Found {yummi_amount} YUMMI tokens")
                    if yummi_amount >= MIN_YUMMI_REQUIRED:
                        return True, "Sufficient YUMMI balance"
                    return False, f"Insufficient YUMMI balance (has {yummi_amount:,}, needs {MIN_YUMMI_REQUIRED:,})"
            
            return False, "No YUMMI tokens found in wallet"
            
        except blockfrost.ApiError as e:
            logger.error(f"Error checking assets: {str(e)}")
            if e.status_code == 429:
                return False, "Service temporarily unavailable (rate limit)"
            return False, "Could not check wallet assets"
            
    except Exception as e:
        logger.error(f"Error checking YUMMI balance: {str(e)}")
        return False, "An unexpected error occurred"

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

        has_balance, message = check_yummi_balance(wallet_address)
        if not has_balance:
            embed = discord.Embed(
                title="❌ Wallet Check Failed",
                description=message,
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
            return

        try:
            bot.db.add_user(str(interaction.user.id))
            if bot.db.add_wallet(str(interaction.user.id), wallet_address):
                bot.db.update_wallet_status(wallet_address, True)
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

bot = WalletBud()

@bot.tree.command(name="addwallet", description="Add a Cardano wallet for tracking (requires 20,000 YUMMI tokens)")
async def add_wallet(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.DMChannel):
        await interaction.response.send_message("Please use this command in DMs!", ephemeral=True)
        return
    await interaction.response.send_modal(WalletModal())

@bot.tree.command(name="list_wallets", description="List your registered wallets")
async def list_wallets(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.DMChannel):
        await interaction.response.send_message("Please use this command in DMs!", ephemeral=True)
        return
        
    wallets = bot.db.get_user_wallets(str(interaction.user.id))
    if not wallets:
        await interaction.response.send_message("You don't have any registered wallets.")
        return

    embed = discord.Embed(
        title="Your Registered Wallets",
        color=discord.Color.blue()
    )
    
    for wallet in wallets:
        has_balance, message = check_yummi_balance(wallet)
        status = "✅ Active" if has_balance else "❌ Inactive"
        embed.add_field(
            name=f"Wallet ({status})",
            value=f"`{wallet[:8]}...{wallet[-8:]}`\n{message}",
            inline=False
        )

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove_wallet", description="Remove a wallet from tracking")
async def remove_wallet(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.DMChannel):
        await interaction.response.send_message("Please use this command in DMs!", ephemeral=True)
        return

    wallets = bot.db.get_user_wallets(str(interaction.user.id))
    if not wallets:
        await interaction.response.send_message("You don't have any registered wallets.")
        return

    class WalletSelect(discord.ui.Select):
        def __init__(self, wallets):
            options = []
            for wallet in wallets:
                truncated = f"{wallet[:8]}...{wallet[-8:]}"
                options.append(discord.SelectOption(
                    label=truncated,
                    value=wallet,
                    description="Click to remove this wallet"
                ))
            
            super().__init__(
                placeholder="Select a wallet to remove...",
                min_values=1,
                max_values=1,
                options=options
            )

        async def callback(self, interaction: discord.Interaction):
            wallet_address = self.values[0]
            try:
                if bot.db.remove_wallet(str(interaction.user.id), wallet_address):
                    embed = discord.Embed(
                        title="✅ Wallet Removed",
                        description="The wallet has been removed from tracking.",
                        color=discord.Color.green()
                    )
                    embed.add_field(
                        name="Wallet Address",
                        value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
                        inline=False
                    )
                    await interaction.response.send_message(embed=embed)
                else:
                    await interaction.response.send_message("❌ Failed to remove wallet. Please try again.")
            except Exception as e:
                logger.error(f"Error removing wallet: {str(e)}")
                await interaction.response.send_message("An error occurred. Please try again.")

    class WalletSelectView(discord.ui.View):
        def __init__(self, wallets):
            super().__init__()
            self.add_item(WalletSelect(wallets))

    view = WalletSelectView(wallets)
    await interaction.response.send_message("Select a wallet to remove:", view=view)

if __name__ == "__main__":
    bot.run(token)
