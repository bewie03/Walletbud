import os
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from database import Database
import blockfrost
from datetime import datetime, timedelta
from config import YUMMI_POLICY_ID, REQUIRED_BUD_TOKENS

load_dotenv()

# Bot setup with DM permissions
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True

bot = discord.Bot(intents=intents)
db = Database()

# Constants
YUMMI_ASSET_ID = YUMMI_POLICY_ID
MIN_YUMMI_REQUIRED = REQUIRED_BUD_TOKENS

# Blockfrost setup
project_id = os.getenv('BLOCKFROST_API_KEY')
blockfrost_client = blockfrost.BlockFrostApi(
    project_id=project_id,
    base_url='https://cardano-mainnet.blockfrost.io/api/v0'
)

@bot.event
async def on_ready():
    print(f'{bot.user} is ready and online!')
    try:
        print("Starting to sync commands...")
        commands = await bot.sync_commands(guild=None)  # Set to None for global commands
        print(f"Synced {len(commands)} commands globally")
    except Exception as e:
        print(f"Error syncing commands: {e}")
    check_wallets.start()

def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        # Get all assets in the wallet
        assets = blockfrost_client.address_assets(wallet_address)
        
        # Look for YUMMI token
        for asset in assets:
            if asset.unit == YUMMI_ASSET_ID:
                # Convert quantity to actual token amount (considering decimals)
                yummi_amount = int(asset.quantity)
                print(f"Found {yummi_amount} YUMMI tokens in wallet {wallet_address}")
                return yummi_amount >= MIN_YUMMI_REQUIRED
        
        # If we get here, no YUMMI tokens were found
        print(f"No YUMMI tokens found in wallet {wallet_address}")
        return False
    except Exception as e:
        print(f"Error checking YUMMI balance: {e}")
        return False

@bot.slash_command(
    name="addwallet",
    description="Add a Cardano wallet for tracking (requires 20,000 YUMMI tokens)",
    guild_ids=None,
    dm_permission=True
)
async def add_wallet(ctx):
    class WalletModal(discord.ui.Modal):
        def __init__(self):
            super().__init__(title="Add Wallet")
            self.wallet = discord.ui.InputText(
                label="Wallet Address",
                placeholder="Enter your Cardano wallet address",
                style=discord.InputTextStyle.short
            )
            self.add_item(self.wallet)

        async def callback(self, interaction: discord.Interaction):
            wallet_address = self.wallet.value
            
            # Validate wallet address format
            if not wallet_address.startswith(('addr1', 'addr_test1')):
                await interaction.response.send_message("Invalid wallet address format. Please provide a valid Cardano address.")
                return

            try:
                # First verify wallet exists
                try:
                    blockfrost_client.address(wallet_address)
                except Exception as e:
                    await interaction.response.send_message("Invalid wallet address or unable to verify wallet. Please check the address and try again.")
                    return

                # Check YUMMI token balance
                if not check_yummi_balance(wallet_address):
                    embed = discord.Embed(
                        title="❌ Insufficient YUMMI Balance",
                        description=f"Your wallet needs at least {MIN_YUMMI_REQUIRED:,} YUMMI tokens to use this bot.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="YUMMI Token",
                        value=f"`{YUMMI_ASSET_ID}`",
                        inline=False
                    )
                    await interaction.response.send_message(embed=embed)
                    return

                # If we get here, wallet has enough YUMMI tokens
                db.add_user(str(interaction.user.id))
                if db.add_wallet(str(interaction.user.id), wallet_address):
                    db.update_wallet_status(wallet_address, True)
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
                    embed.add_field(
                        name="YUMMI Balance",
                        value="✅ Sufficient balance",
                        inline=False
                    )
                    await interaction.response.send_message(embed=embed)
                else:
                    await interaction.response.send_message("Error adding wallet. This wallet might already be registered.")
            except Exception as e:
                await interaction.response.send_message(f"An error occurred while adding the wallet: {str(e)}")

    modal = WalletModal()
    await ctx.send_modal(modal)

@tasks.loop(minutes=5)
async def check_wallets():
    active_wallets = db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
            # First check if wallet still has enough YUMMI tokens
            if not check_yummi_balance(wallet_address):
                print(f"Wallet {wallet_address} no longer has enough YUMMI tokens")
                db.update_wallet_status(wallet_address, False)
                user = await bot.fetch_user(int(discord_id))
                if user:
                    embed = discord.Embed(
                        title="❌ Wallet Tracking Disabled",
                        description=f"Your wallet no longer has the required {MIN_YUMMI_REQUIRED:,} YUMMI tokens.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
                        inline=False
                    )
                    await user.send(embed=embed)
                continue

            # Get transactions from the last 5 minutes
            now = datetime.now()
            five_mins_ago = now - timedelta(minutes=5)
            
            txs = blockfrost_client.address_transactions(
                wallet_address,
                from_block=str(int(five_mins_ago.timestamp()))
            )
            
            if txs:
                user = await bot.fetch_user(int(discord_id))
                if user:
                    for tx in txs:
                        try:
                            # Get transaction details
                            tx_details = blockfrost_client.transaction(tx.tx_hash)
                            embed = discord.Embed(
                                title="New Transaction Detected! 🔔",
                                color=discord.Color.blue()
                            )
                            embed.add_field(
                                name="Wallet",
                                value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
                                inline=False
                            )
                            embed.add_field(
                                name="Transaction ID",
                                value=f"`{tx.tx_hash}`",
                                inline=False
                            )
                            embed.add_field(
                                name="Amount",
                                value=f"`{tx_details.output_amount[0].quantity / 1000000:.6f} ADA`",
                                inline=False
                            )
                            embed.set_footer(text="WalletBud Notification")
                            
                            await user.send(embed=embed)
                        except discord.Forbidden:
                            print(f"Cannot send DM to user {discord_id}")
                        except Exception as e:
                            print(f"Error getting transaction details: {e}")
                        
        except Exception as e:
            print(f"Error checking wallet {wallet_address}: {e}")

bot.run(os.getenv('DISCORD_TOKEN'))
