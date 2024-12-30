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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Check if token is loaded
token = os.getenv('DISCORD_TOKEN')
if not token:
    logger.error("No Discord token found! Make sure DISCORD_TOKEN is set in .env")
    exit(1)
else:
    logger.info("Discord token loaded successfully")

# Bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.messages = True
intents.members = True

# Create bot instance
class WalletBud(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.db = Database()
        
    async def setup_hook(self):
        await self.tree.sync()
        self.check_wallets.start()

bot = WalletBud()

# Constants
YUMMI_POLICY_ID = YUMMI_POLICY_ID
MIN_YUMMI_REQUIRED = REQUIRED_BUD_TOKENS

# Blockfrost setup
project_id = os.getenv('BLOCKFROST_API_KEY')
blockfrost_client = blockfrost.BlockFrostApi(
    project_id=project_id,
    base_url='https://cardano-mainnet.blockfrost.io/api/v0'
)

@bot.event
async def on_ready():
    logger.info(f'Bot {bot.user} is ready and online!')
    try:
        # Set bot presence
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="YUMMI wallets | DM me!"
            )
        )
        logger.info("Bot presence set")
    except Exception as e:
        logger.error(f"Error during startup: {e}")

def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        logger.info(f"Checking wallet {wallet_address} for YUMMI tokens")
        
        # First verify the wallet exists
        try:
            wallet = blockfrost_client.address(wallet_address)
            logger.info(f"Wallet verified: {wallet_address}")
        except Exception as e:
            logger.error(f"Error verifying wallet: {str(e)}")
            return False, "Could not verify wallet"

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
            
            return False, "No YUMMI tokens found"
            
        except Exception as e:
            logger.error(f"Error checking assets: {str(e)}")
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

@tasks.loop(minutes=TRANSACTION_CHECK_INTERVAL)
async def check_wallets():
    """Check all active wallets for new transactions"""
    active_wallets = bot.db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
            # Check YUMMI balance
            has_balance, message = check_yummi_balance(wallet_address)
            if not has_balance:
                logger.info(f"Deactivating wallet {wallet_address}: {message}")
                bot.db.update_wallet_status(wallet_address, False)
                try:
                    user = await bot.fetch_user(int(discord_id))
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
                        
                        user = await bot.fetch_user(int(discord_id))
                        if user:
                            await user.send(embed=embed)
                            
                    except Exception as e:
                        logger.error(f"Error processing transaction {tx.tx_hash}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error checking transactions for {wallet_address}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing wallet {wallet_address}: {str(e)}")

if __name__ == "__main__":
    bot.run(token)
