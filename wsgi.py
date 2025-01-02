import asyncio
from aiohttp import web
from bot import WalletBudBot
from config import DISCORD_TOKEN
import logging
import os

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
        logger.info("Starting bot in startup handler...")
        logger.info("Calling bot.start() with Discord token...")
        await bot.start(DISCORD_TOKEN)
        logger.info("Bot started successfully in startup handler")
    except Exception as e:
        logger.error(f"Failed to start bot in startup handler: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        raise

# Cleanup on shutdown
async def cleanup(app):
    """Clean up when the app shuts down"""
    try:
        logger.info("Starting cleanup in shutdown handler...")
        await bot.close()
        logger.info("Bot shutdown complete in cleanup handler")
    except Exception as e:
        logger.error(f"Error during shutdown in cleanup handler: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        if hasattr(e, '__dict__'):
            logger.error(f"Error details: {e.__dict__}")
        raise

# Register startup and shutdown handlers
logger.info("Registering startup and shutdown handlers...")
app.on_startup.append(start_bot)
app.on_cleanup.append(cleanup)
logger.info("Handlers registered")

logger.info("wsgi.py initialization complete")

# Run the application
if __name__ == "__main__":
    logger.info("Starting aiohttp application...")
    web.run_app(app, port=int(os.environ.get("PORT", 8080)))
    logger.info("Application started")
