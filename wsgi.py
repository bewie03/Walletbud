import os
import signal
import asyncio
import logging
from aiohttp import web
from bot import WalletBudBot
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize bot instance
bot = WalletBudBot()

async def health_check(request):
    """Health check endpoint to monitor application status"""
    try:
        health_data = {
            "status": "healthy",
            "bot_latency": round(bot.latency * 1000, 2) if bot.is_ready() else None,
            "connected": bot.is_ready(),
            "guilds": len(bot.guilds) if bot.is_ready() else 0
        }
        return web.json_response(health_data)
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return web.json_response({"status": "unhealthy", "error": str(e)}, status=500)

async def start_bot():
    """Start the Discord bot"""
    try:
        await bot.start(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}")
        raise

async def cleanup(app):
    """Cleanup function to handle graceful shutdown"""
    logger.info("Shutting down bot and web server...")
    await bot.close()
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()

def signal_handler():
    """Handle system signals for graceful shutdown"""
    logger.info("Received shutdown signal")
    asyncio.get_event_loop().stop()

async def init_app():
    """Initialize the web application"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    
    # Add cleanup callback
    app.on_cleanup.append(cleanup)
    
    # Start bot in background
    asyncio.create_task(start_bot())
    
    return app

def get_app():
    """Get the web application instance"""
    # Explicitly mark this as an aiohttp application factory
    return init_app

if __name__ == '__main__':
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: signal_handler())
    
    # Run the application
    web.run_app(get_app(), port=int(os.getenv('PORT', 8080)))
