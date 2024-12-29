import os
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from database import Database
import blockfrost
from datetime import datetime, timedelta

load_dotenv()

# Bot setup with DM permissions
intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True

bot = discord.Bot(intents=intents)
db = Database()

# Blockfrost setup
project_id = os.getenv('BLOCKFROST_API_KEY')
blockfrost_client = blockfrost.BlockFrostApi(
    project_id=project_id,
    base_url='https://cardano-mainnet.blockfrost.io/api'
)

@bot.event
async def on_ready():
    print(f'{bot.user} is ready and online!')
    check_wallets.start()

def has_minimum_bud(address):
    try:
        assets = blockfrost_client.address_assets(address)
        for asset in assets:
            # Replace with actual BUD policy ID and asset name
            if asset.unit == "BUD_POLICY_ID.BUD":  # Update this with actual BUD token ID
                return asset.quantity >= 20000
        return False
    except Exception as e:
        print(f"Error checking BUD balance: {e}")
        return False

@bot.slash_command(name="addwallet", description="Add a Cardano wallet for tracking")
async def add_wallet(ctx, wallet_address: str):
    # Allow command only in DMs
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.respond("This command can only be used in DMs!", ephemeral=True)
        return

    if has_minimum_bud(wallet_address):
        db.add_user(str(ctx.author.id))
        if db.add_wallet(str(ctx.author.id), wallet_address):
            db.update_wallet_status(wallet_address, True)
            await ctx.respond(f"Wallet {wallet_address} added successfully! You will receive DM notifications for transactions.")
        else:
            await ctx.respond("Error adding wallet. Please try again.")
    else:
        await ctx.respond("This wallet doesn't have the minimum required BUD tokens (20,000).")

@tasks.loop(minutes=5)
async def check_wallets():
    active_wallets = db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
            # Get transactions from the last 5 minutes
            now = datetime.now()
            txs = blockfrost_client.address_transactions(
                wallet_address,
                from_block=str(int((now - timedelta(minutes=5)).timestamp()))
            )
            
            if txs:
                user = await bot.fetch_user(int(discord_id))
                if user:
                    for tx in txs:
                        # Send DM notification
                        message = f"New transaction in wallet {wallet_address}:\nTransaction ID: {tx.tx_hash}"
                        try:
                            await user.send(message)
                        except discord.Forbidden:
                            print(f"Cannot send DM to user {discord_id}")
                        
        except Exception as e:
            print(f"Error checking wallet {wallet_address}: {e}")

bot.run(os.getenv('DISCORD_TOKEN'))
