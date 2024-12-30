import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

token = os.getenv('DISCORD_TOKEN')
if not token:
    raise ValueError("No token found!")

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    logger.info(f"{bot.user} is online!")
