import asyncio
from aiohttp import web
from bot import WalletBudBot
from config import DISCORD_TOKEN
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create aiohttp app
app = web.Application()

# Initialize bot instance
bot = WalletBudBot()

# Add webhook route
app.router.add_post('/webhook', bot.handle_webhook)

# Start the bot
async def start_bot(app):
    """Start the bot when the app starts"""
    try:
        logger.info("Starting bot...")
        await bot.start(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

# Cleanup on shutdown
async def cleanup(app):
    """Clean up when the app shuts down"""
    try:
        logger.info("Shutting down bot...")
        await bot.close()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Register startup and shutdown handlers
app.on_startup.append(start_bot)
app.on_cleanup.append(cleanup)
