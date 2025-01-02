import asyncio
import logging
import os
import signal
import sys
from aiohttp import web
from aiohttp_wsgi import WSGIHandler
from bot import WalletBudBot
from config import DISCORD_TOKEN

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Starting wsgi.py initialization")

# Initialize bot instance
logger.info("Creating bot instance...")
bot = WalletBudBot()
logger.info("Bot instance created")

# Create aiohttp app
app = web.Application()
logger.info("Created aiohttp app")

# Add webhook route
logger.info("Adding webhook route...")
app.router.add_post('/webhook', bot.handle_webhook)
logger.info("Webhook route added")

# Add health check route
async def health_check(request):
    """Health check endpoint"""
    try:
        health_data = {
            'status': 'healthy',
            'bot_connected': bot.is_ready(),
            'latency': round(bot.latency * 1000, 2) if bot.is_ready() else None,
            'guild_count': len(bot.guilds) if bot.is_ready() else 0,
        }
        
        # Check connections
        connections = await bot.check_connections()
        health_data['connections'] = connections
        
        return web.json_response(health_data)
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return web.json_response({
            'status': 'unhealthy',
            'error': str(e)
        }, status=500)

app.router.add_get('/health', health_check)
logger.info("Health check route added")

# Signal handlers
def handle_exit(signame, app):
    """Handle exit signals"""
    async def _cleanup():
        logger.info(f"Received exit signal {signame}")
        await cleanup(app)
        sys.exit(0)
    
    loop = asyncio.get_event_loop()
    loop.create_task(_cleanup())

# Start the bot
async def start_bot(app):
    """Start the bot when the app starts"""
    try:
        logger.info("Starting bot in startup handler...")
        
        # Set up signal handlers
        for signame in ('SIGINT', 'SIGTERM'):
            if hasattr(app, 'loop'):
                app.loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda s=signame: handle_exit(s, app)
                )
        
        # Start the bot in a background task
        app['bot_task'] = asyncio.create_task(bot.start(DISCORD_TOKEN))
        logger.info("Bot start task created")
        
        # Wait a bit to ensure bot connects
        try:
            await asyncio.wait_for(bot.wait_until_ready(), timeout=30)
            logger.info("Bot successfully connected to Discord")
        except asyncio.TimeoutError:
            logger.warning("Bot did not connect within timeout period")
        
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
        
        # Cancel bot task if it exists
        if 'bot_task' in app:
            logger.info("Cancelling bot task...")
            app['bot_task'].cancel()
            try:
                await app['bot_task']
            except asyncio.CancelledError:
                pass
        
        # Close Discord bot
        if hasattr(app, 'bot') and app.bot is not None:
            logger.info("Closing Discord bot...")
            await app.bot.close()
            logger.info("Discord bot closed")
        
        # Close any remaining connections
        if hasattr(app, 'cleanup_ctx'):
            logger.info("Running cleanup contexts...")
            for cleanup_ctx in app.cleanup_ctx:
                try:
                    await cleanup_ctx(app)
                except Exception as e:
                    logger.error(f"Error in cleanup context: {e}")
        
        logger.info("Cleanup complete")
        
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

# Store bot instance in app for access in handlers
app.bot = bot

logger.info("wsgi.py initialization complete")

# Create WSGI application for gunicorn
def create_wsgi_app():
    """Create WSGI application for gunicorn"""
    logger.info("Creating WSGI application...")
    wsgi_handler = WSGIHandler(app)
    return wsgi_handler.handle_request

# This is needed for gunicorn to find the application
application = create_wsgi_app()

# Run the application directly if not using gunicorn
if __name__ == "__main__":
    logger.info("Starting aiohttp application directly...")
    try:
        # Create new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app.loop = loop
        
        # Run the application
        port = int(os.environ.get("PORT", 8080))
        logger.info(f"Starting server on port {port}")
        web.run_app(app, port=port, loop=loop, access_log_format='%a %l %u %t "%r" %s %b "%{Referer}i" "%{User-Agent}i"')
        logger.info("Application started")
        
    except Exception as e:
        logger.error(f"Error starting application: {str(e)}")
        raise
        
    finally:
        # Clean up the event loop
        loop.close()
