import os
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from database import Database
import blockfrost
from datetime import datetime, timedelta
from config import YUMMI_POLICY_ID, REQUIRED_BUD_TOKENS, TRANSACTION_CHECK_INTERVAL, MAX_TX_HISTORY

load_dotenv()

# Bot setup with DM permissions
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.messages = True

bot = discord.Bot(intents=intents)
db = Database()

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
    print(f'{bot.user} is ready and online!')
    try:
        # Set bot presence
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="YUMMI wallets 👀"
            ),
            status=discord.Status.online
        )
        print("Bot presence set")
        
        # Sync commands
        print("Starting to sync commands...")
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands")
        
        # Start wallet checking
        check_wallets.start()
        print("Wallet checking started")
    except Exception as e:
        print(f"Error during startup: {e}")

@bot.event
async def on_application_command_error(ctx, error):
    """Handle command errors gracefully"""
    if isinstance(error, discord.app_commands.CommandOnCooldown):
        await ctx.response.send_message(f"This command is on cooldown. Try again in {error.retry_after:.2f} seconds.")
    elif isinstance(error, discord.app_commands.MissingPermissions):
        await ctx.response.send_message("You don't have permission to use this command.")
    else:
        print(f"Command error: {str(error)}")
        await ctx.response.send_message("An error occurred while processing your command. Please try again later.")

def check_yummi_balance(wallet_address):
    """Check if wallet has required amount of YUMMI tokens"""
    try:
        print(f"Checking wallet {wallet_address} for YUMMI tokens")
        print(f"Using policy ID: {YUMMI_POLICY_ID}")
        
        # First verify the wallet exists
        try:
            wallet = blockfrost_client.address(wallet_address)
            print(f"Wallet verified: {wallet_address}")
        except Exception as e:
            print(f"Error verifying wallet: {str(e)}")
            if hasattr(e, 'status_code'):
                if e.status_code == 404:
                    return False, "Wallet address not found"
                elif e.status_code == 402:
                    return False, "API rate limit exceeded"
                elif e.status_code == 403:
                    return False, "API authentication failed"
            return False, "Could not verify wallet"

        # Get all assets in the wallet (handle pagination)
        try:
            all_assets = []
            page = 1
            while True:
                assets = blockfrost_client.address_assets(wallet_address, params={'page': page})
                if not assets:
                    break
                all_assets.extend(assets)
                page += 1
                
            print(f"Found {len(all_assets)} total assets in wallet")
            
            # Look for any asset with the YUMMI policy ID (case insensitive)
            policy_id = YUMMI_POLICY_ID.lower()
            for asset in all_assets:
                print(f"Checking asset: {asset.unit}")
                if asset.unit.lower().startswith(policy_id):
                    # Convert quantity from string to integer
                    try:
                        yummi_amount = int(asset.quantity)
                        print(f"Found YUMMI token with amount: {yummi_amount}")
                        if yummi_amount >= MIN_YUMMI_REQUIRED:
                            return True, "Sufficient YUMMI balance"
                        else:
                            return False, f"Insufficient YUMMI balance (has {yummi_amount:,}, needs {MIN_YUMMI_REQUIRED:,})"
                    except ValueError:
                        print(f"Error converting token quantity: {asset.quantity}")
                        return False, "Error reading token balance"
            
            return False, "No YUMMI tokens found in wallet"
            
        except Exception as e:
            print(f"Error checking assets: {str(e)}")
            return False, "Could not check wallet assets"
            
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False, "An unexpected error occurred"

@bot.tree.command(
    name="addwallet",
    description="Add a Cardano wallet for tracking (requires 20,000 YUMMI tokens)"
)
async def add_wallet(interaction: discord.Interaction):
    """Add a new wallet for tracking"""
    modal = WalletModal()
    await interaction.response.send_modal(modal)

class WalletModal(discord.ui.Modal, title="Add Wallet"):
    wallet = discord.ui.TextInput(
        label="Wallet Address",
        placeholder="Enter your Cardano wallet address",
        style=discord.TextStyle.short,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        wallet_address = self.wallet.value
        
        # Validate wallet address format
        if not wallet_address.startswith(('addr1', 'addr_test1')):
            await interaction.response.send_message("Invalid wallet address format. Please provide a valid Cardano address.")
            return

        # Check YUMMI balance with improved error handling
        has_balance, message = check_yummi_balance(wallet_address)
        
        if not has_balance:
            embed = discord.Embed(
                title="❌ Wallet Check Failed",
                description=message,
                color=discord.Color.red()
            )
            embed.add_field(
                name="YUMMI Token Policy ID",
                value=f"`{YUMMI_POLICY_ID}`",
                inline=False
            )
            await interaction.response.send_message(embed=embed)
            return

        # If we get here, wallet has enough YUMMI tokens
        try:
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
                await interaction.response.send_message(embed=embed)
            else:
                await interaction.response.send_message("Error adding wallet. This wallet might already be registered.")
        except Exception as e:
            await interaction.response.send_message(f"An error occurred while adding the wallet: {str(e)}")

class WalletSelectView(discord.ui.View):
    def __init__(self, wallets):
        super().__init__()
        self.add_item(WalletSelect(wallets))

class WalletSelect(discord.ui.Select):
    def __init__(self, wallets):
        options = []
        for wallet in wallets:
            # Create a truncated version for display
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
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            wallet_address = self.values[0]
            if db.remove_wallet(str(interaction.user.id), wallet_address):
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
            await interaction.response.send_message(f"An error occurred: {str(e)}")

@bot.tree.command(
    name="list_wallets",
    description="List all your registered wallets and their status"
)
async def list_wallets(interaction: discord.Interaction):
    """List all wallets registered to the user"""
    try:
        wallets = db.get_user_wallets(str(interaction.user.id))
        if not wallets:
            await interaction.response.send_message("You don't have any registered wallets.")
            return

        embed = discord.Embed(
            title="🏦 Your Registered Wallets",
            description=f"You have {len(wallets)} registered wallet(s)",
            color=discord.Color.blue()
        )

        for wallet in wallets:
            # Get wallet status and YUMMI balance
            has_balance, _ = check_yummi_balance(wallet)
            status = "✅ Active" if has_balance else "❌ Inactive"
            
            # Get last checked time
            last_checked = db.get_last_checked(wallet)
            last_checked_str = last_checked.strftime("%Y-%m-%d %H:%M UTC") if last_checked else "Never"
            
            # Add wallet field
            embed.add_field(
                name=f"Wallet ({status})",
                value=f"Address: `{wallet[:8]}...{wallet[-8:]}`\nLast Checked: {last_checked_str}",
                inline=False
            )

        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"An error occurred: {str(e)}")

@bot.tree.command(
    name="remove_wallet",
    description="Remove a wallet from tracking"
)
async def remove_wallet(interaction: discord.Interaction):
    """Remove a wallet from tracking using a dropdown menu"""
    try:
        # Get user's wallets
        wallets = db.get_user_wallets(str(interaction.user.id))
        if not wallets:
            await interaction.response.send_message("You don't have any registered wallets to remove.")
            return

        # Create the view with the wallet selection dropdown
        view = WalletSelectView(wallets)
        await interaction.response.send_message(
            "Select the wallet you want to remove:",
            view=view
        )
    except Exception as e:
        await interaction.response.send_message(f"An error occurred: {str(e)}")

async def process_transaction(wallet_address, discord_id, tx_hash):
    """Process a single transaction and send notification if relevant"""
    try:
        # Get transaction details
        tx = blockfrost_client.transaction(tx_hash)
        utxos = blockfrost_client.transaction_utxos(tx_hash)
        
        # Determine if transaction is incoming or outgoing
        is_sender = any(input['address'] == wallet_address for input in utxos.inputs)
        is_receiver = any(output['address'] == wallet_address for output in utxos.outputs)
        
        if not (is_sender or is_receiver):
            return
            
        # Get the relevant amounts
        amounts = []
        if is_sender:
            # Find outputs that aren't back to the sender (change)
            for output in utxos.outputs:
                if output['address'] != wallet_address:
                    amounts.extend(output['amount'])
        else:
            # Find outputs to this wallet
            for output in utxos.outputs:
                if output['address'] == wallet_address:
                    amounts.extend(output['amount'])
        
        # Create notification embed
        embed = discord.Embed(
            title="🔔 Transaction Detected!",
            description=f"{'Outgoing ↗️' if is_sender else 'Incoming ↙️'} Transaction",
            color=discord.Color.red() if is_sender else discord.Color.green(),
            timestamp=datetime.fromtimestamp(tx.block_time)
        )
        
        # Add amount details
        for amount in amounts:
            if amount['unit'] == 'lovelace':
                ada_amount = float(amount['quantity']) / 1_000_000
                embed.add_field(
                    name="ADA",
                    value=f"{ada_amount:,.2f} ₳",
                    inline=True
                )
            else:
                # Handle other assets (including YUMMI)
                asset_policy = amount['unit'][:56]  # First 56 chars are policy ID
                if asset_policy.lower() == YUMMI_POLICY_ID.lower():
                    embed.add_field(
                        name="YUMMI",
                        value=f"{int(amount['quantity']):,}",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="Other Asset",
                        value=f"{int(amount['quantity']):,} units",
                        inline=True
                    )
        
        # Add transaction details
        cardanoscan_url = f"https://cardanoscan.io/transaction/{tx_hash}"
        embed.add_field(
            name="Transaction Details",
            value=f"[View on Cardanoscan]({cardanoscan_url})",
            inline=False
        )
        
        # Add block details
        embed.add_field(
            name="Block",
            value=f"`{tx.block}`",
            inline=True
        )
        
        # Add wallet address (truncated)
        embed.add_field(
            name="Wallet",
            value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
            inline=True
        )
        
        # Send notification to user
        user = await bot.fetch_user(int(discord_id))
        if user:
            await user.send(embed=embed)
            
    except Exception as e:
        print(f"Error processing transaction {tx_hash}: {str(e)}")

@tasks.loop(minutes=TRANSACTION_CHECK_INTERVAL)
async def check_wallets():
    """Check all active wallets for new transactions"""
    active_wallets = db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
            # Update last checked time
            db.update_last_checked(wallet_address)
            
            # First check if wallet still has enough YUMMI tokens
            has_balance, message = check_yummi_balance(wallet_address)
            if not has_balance:
                print(f"Wallet {wallet_address} no longer has enough YUMMI tokens")
                db.update_wallet_status(wallet_address, False)
                user = await bot.fetch_user(int(discord_id))
                if user:
                    embed = discord.Embed(
                        title="❌ Wallet Deactivated",
                        description="Your wallet has been deactivated due to insufficient YUMMI tokens.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="Required Balance",
                        value=f"{MIN_YUMMI_REQUIRED:,} YUMMI",
                        inline=False
                    )
                    embed.add_field(
                        name="Wallet",
                        value=f"`{wallet_address[:8]}...{wallet_address[-8:]}`",
                        inline=False
                    )
                    await user.send(embed=embed)
                continue

            # Get recent transactions
            try:
                transactions = blockfrost_client.address_transactions(
                    wallet_address,
                    params={'order': 'desc'}  # Get newest first
                )
                
                for tx in transactions[:MAX_TX_HISTORY]:  # Check configurable number of recent transactions
                    await process_transaction(wallet_address, discord_id, tx.tx_hash)
                    
            except Exception as e:
                print(f"Error checking transactions for wallet {wallet_address}: {str(e)}")
                
        except Exception as e:
            print(f"Error processing wallet {wallet_address}: {str(e)}")

bot.run(os.getenv('DISCORD_TOKEN'))
