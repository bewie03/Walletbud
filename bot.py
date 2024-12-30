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
            # First verify wallet is still valid
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

            # Get last checked time
            last_checked = self.db.get_last_checked(wallet_address)
            if not last_checked:
                last_checked = datetime.utcnow() - timedelta(minutes=TRANSACTION_CHECK_INTERVAL)

            try:
                # Get transactions using /addresses/{address}/transactions endpoint
                transactions = blockfrost_client.address_transactions(
                    wallet_address,
                    from_block=None,  # Get all recent transactions
                    gather_pages=True  # Get all pages
                )
                
                logger.info(f"Found {len(transactions)} transactions for wallet {wallet_address}")
                
                # Process each transaction
                for tx in transactions:
                    # Get detailed transaction info using /txs/{hash} endpoint
                    tx_details = blockfrost_client.transaction(tx.tx_hash)
                    tx_time = datetime.fromtimestamp(tx_details.block_time)
                    
                    # Only process transactions after last check
                    if tx_time <= last_checked:
                        continue
                        
                    logger.info(f"Processing transaction {tx.tx_hash} from {tx_time}")
                    
                    # Get transaction UTXOs
                    tx_utxos = blockfrost_client.transaction_utxos(tx.tx_hash)
                    
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
                    
                    # Create notification embed
                    try:
                        user = await self.fetch_user(int(discord_id))
                        if user:
                            embed = discord.Embed(
                                title="🔔 New Transaction Detected",
                                description=f"Transaction: [{tx.tx_hash}](https://cardanoscan.io/transaction/{tx.tx_hash})",
                                color=discord.Color.blue(),
                                timestamp=tx_time
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

                # Update last checked time after processing all transactions
                self.db.update_last_checked(wallet_address)

            except blockfrost.ApiError as e:
                logger.error(f"Error fetching transactions: {str(e)}")
                if e.status_code == 429:  # Rate limit
                    return  # Skip this check, will try again next interval
                    
        except Exception as e:
            logger.error(f"Error in check_wallet_transactions: {str(e)}")

# Constants
YUMMI_POLICY_ID = YUMMI_POLICY_ID
MIN_YUMMI_REQUIRED = REQUIRED_BUD_TOKENS

# Initialize Blockfrost client
try:
    blockfrost_project_id = os.getenv('BLOCKFROST_API_KEY')
    if not blockfrost_project_id:
        logger.error("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")
        exit(1)
    blockfrost_client = blockfrost.BlockFrostApi(
        project_id=blockfrost_project_id
    )
    # Test connection with a simple query
    test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
    blockfrost_client.address(test_address)
    logger.info("Successfully connected to Blockfrost API")
except Exception as e:
    logger.error(f"Failed to initialize Blockfrost client: {e}")
    exit(1)

def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        logger.info(f"Checking wallet {wallet_address} for YUMMI tokens")
        
        try:
            # Get wallet assets using /addresses/{address}/total endpoint
            address_info = blockfrost_client.address(wallet_address)
            logger.info(f"Retrieved address info for wallet: {wallet_address}")
            
            # Get specific asset details using /addresses/{address}/utxos
            utxos = blockfrost_client.address_utxos(wallet_address)
            logger.info(f"Retrieved UTXOs for wallet: {wallet_address}")
            
            yummi_amount = 0
            for utxo in utxos:
                for amount in utxo.amount:
                    # Policy ID is the first 56 characters of the asset unit
                    if hasattr(amount, 'unit') and amount.unit.startswith(YUMMI_POLICY_ID):
                        yummi_amount += int(amount.quantity)
                        logger.info(f"Found {amount.quantity} YUMMI tokens in UTXO")
            
            logger.info(f"Total YUMMI tokens found: {yummi_amount}")
            
            if yummi_amount >= REQUIRED_BUD_TOKENS:
                return True, f"Wallet has {yummi_amount} YUMMI tokens"
            else:
                return False, f"Insufficient YUMMI tokens: {yummi_amount}/{REQUIRED_BUD_TOKENS} required"
                
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
                return False, f"API Error: {str(e)}"
                
    except Exception as e:
        logger.error(f"Error checking YUMMI balance: {str(e)}")
        return False, f"Error checking wallet: {str(e)}"

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
