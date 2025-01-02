import asyncio
from aiohttp import web
from bot import WalletBudBot
from config import DISCORD_TOKEN
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Starting wsgi.py initialization")

# Create aiohttp app
app = web.Application()
logger.info("Created aiohttp app")

# Initialize bot instance
logger.info("Creating bot instance...")
bot = WalletBudBot()
logger.info("Bot instance created")

# Add webhook route
logger.info("Adding webhook route...")
app.router.add_post('/webhook', bot.handle_webhook)
logger.info("Webhook route added")

# Start the bot
async def start_bot(app):
    """Start the bot when the app starts"""
    try:
        logger.info("Starting bot...")
        await bot.start(DISCORD_TOKEN)
        logger.info("Bot started successfully")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

# Cleanup on shutdown
async def cleanup(app):
    """Clean up when the app shuts down"""
    try:
        logger.info("Shutting down bot...")
        await bot.close()
        logger.info("Bot shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Register startup and shutdown handlers
logger.info("Registering startup and shutdown handlers...")
app.on_startup.append(start_bot)
app.on_cleanup.append(cleanup)
logger.info("Handlers registered")

logger.info("wsgi.py initialization complete")
