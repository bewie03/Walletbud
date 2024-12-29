import discord
from discord.ext import commands, tasks
import asyncio
from blockfrost import BlockFrostApi
from datetime import datetime
import config
from database import Database

# Initialize Discord bot with intents and disable voice
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=config.COMMAND_PREFIX, intents=intents)
bot.voice_clients = None  # Disable voice functionality

# Initialize Blockfrost API
blockfrost = BlockFrostApi(
    project_id=config.BLOCKFROST_API_KEY,
    base_url=config.BLOCKFROST_BASE_URL
)

# Initialize database
db = Database()

@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    check_wallets.start()

async def check_bud_balance(wallet_address):
    """Check if wallet has required BUD tokens"""
    try:
        # Note: Replace with actual BUD token policy ID
        assets = await blockfrost.account_addresses_assets(wallet_address)
        for asset in assets:
            if asset.unit == "BUD_TOKEN_POLICY_ID":  # Replace with actual policy ID
                return asset.quantity >= config.REQUIRED_BUD_TOKENS
        return False
    except Exception as e:
        print(f"Error checking BUD balance: {e}")
        return False

@bot.command(name='addwallet')
async def add_wallet(ctx, wallet_address: str):
    """Add a wallet to track"""
    try:
        # Verify wallet address format
        if not wallet_address.startswith('addr'):
            await ctx.send("Invalid wallet address format. Please provide a valid Cardano address.")
            return

        # Check if user has required BUD tokens
        has_tokens = await check_bud_balance(wallet_address)
        if not has_tokens:
            await ctx.send(f"Insufficient BUD tokens. You need at least {config.REQUIRED_BUD_TOKENS:,} BUD tokens to track this wallet.")
            return

        # Add wallet to database
        db.add_user(str(ctx.author.id))
        if db.add_wallet(str(ctx.author.id), wallet_address):
            db.update_wallet_status(wallet_address, True)
            await ctx.send(f"Successfully added wallet {wallet_address} for tracking!")
        else:
            await ctx.send("Error adding wallet. This wallet might already be registered.")

    except Exception as e:
        await ctx.send(f"An error occurred: {str(e)}")

@tasks.loop(seconds=config.POLLING_INTERVAL)
async def check_wallets():
    """Check all active wallets for new transactions"""
    active_wallets = db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
            # Check if wallet still meets token requirements
            has_tokens = await check_bud_balance(wallet_address)
            if not has_tokens:
                db.update_wallet_status(wallet_address, False)
                user = await bot.fetch_user(int(discord_id))
                await user.send(f"Wallet {wallet_address} has insufficient BUD tokens. Tracking disabled.")
                continue

            # Get recent transactions
            transactions = await blockfrost.address_transactions(wallet_address)
            
            # Process new transactions and send notifications
            for tx in transactions[:5]:  # Limit to recent 5 transactions
                # Add your transaction processing logic here
                user = await bot.fetch_user(int(discord_id))
                await user.send(f"New transaction detected for wallet {wallet_address}:\nTransaction ID: {tx.tx_hash}")

        except Exception as e:
            print(f"Error checking wallet {wallet_address}: {e}")

def run_bot():
    """Run the Discord bot"""
    bot.run(config.DISCORD_TOKEN)

if __name__ == "__main__":
    run_bot()
