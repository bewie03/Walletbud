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
    base_url='https://cardano-mainnet.blockfrost.io/api/v0'
)

@bot.event
async def on_ready():
    print(f'{bot.user} is ready and online!')
    try:
        synced = await bot.sync_commands()
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print(f"Error syncing commands: {e}")
    check_wallets.start()

@bot.slash_command(name="addwallet", description="Add a Cardano wallet for tracking")
async def add_wallet(ctx, wallet_address: str):
    # Allow command only in DMs
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.respond("This command can only be used in DMs!", ephemeral=True)
        return

    # Validate wallet address format
    if not wallet_address.startswith(('addr1', 'addr_test1')):
        await ctx.respond("Invalid wallet address format. Please provide a valid Cardano address.")
        return

    try:
        # Verify the wallet exists by trying to fetch its details
        try:
            blockfrost_client.address(wallet_address)
        except Exception as e:
            await ctx.respond("Invalid wallet address or unable to verify wallet. Please check the address and try again.")
            return

        db.add_user(str(ctx.author.id))
        if db.add_wallet(str(ctx.author.id), wallet_address):
            db.update_wallet_status(wallet_address, True)
            await ctx.respond(f"Wallet {wallet_address} added successfully! You will receive DM notifications for transactions.")
        else:
            await ctx.respond("Error adding wallet. This wallet might already be registered.")
    except Exception as e:
        await ctx.respond(f"An error occurred while adding the wallet: {str(e)}")

@tasks.loop(minutes=5)
async def check_wallets():
    active_wallets = db.get_all_active_wallets()
    
    for wallet_address, discord_id in active_wallets:
        try:
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
                            message = (
                                f" New transaction in wallet {wallet_address[:8]}...{wallet_address[-8:]}\n"
                                f"Transaction ID: {tx.tx_hash}\n"
                                f"Amount: {tx_details.output_amount[0].quantity / 1000000:.6f} ADA"
                            )
                            await user.send(message)
                        except discord.Forbidden:
                            print(f"Cannot send DM to user {discord_id}")
                        except Exception as e:
                            print(f"Error getting transaction details: {e}")
                        
        except Exception as e:
            print(f"Error checking wallet {wallet_address}: {e}")

bot.run(os.getenv('DISCORD_TOKEN'))
