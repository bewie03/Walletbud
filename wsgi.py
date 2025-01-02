import os
import ssl
import signal
import asyncio
import logging
import aiohttp
from aiohttp import web, TCPConnector
from bot import WalletBudBot
from dotenv import load_dotenv
from typing import Optional, Dict, Any
import orjson
from functools import partial
from datetime import datetime
import psutil

# Configure logging
def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.WARNING,  # Set base level to WARNING
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Only show INFO and above for our app's logger
    logger = logging.getLogger('walletbud')
    logger.setLevel(logging.INFO)
    
    # Set third-party loggers to WARNING or higher
    for logger_name in ['aiohttp', 'discord', 'websockets', 'asyncio']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

setup_logging()

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global instances
_init_lock = asyncio.Lock()
bot: Optional[WalletBudBot] = None
app: Optional[web.Application] = None

async def init_app():
    """Initialize the web application"""
    global app, bot
    
    async with _init_lock:
        if app is not None:
            return app
        
        # Create new application instance with optimized client config
        app = web.Application(
            client_max_size=1024**2 * 50,  # 50MB max request size
            handler_args={
                'tcp_keepalive': True,
                'keepalive_timeout': 75.0,  # Heroku's timeout is 55s
            }
        )
        
        # Initialize bot if not already initialized
        if bot is None:
            logger.info("Initializing bot instance...")
            try:
                bot = WalletBudBot()
                bot.app = app  # Pass app instance to bot
                
                # Configure bot's client session with optimized settings
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                connector = TCPConnector(
                    limit=100,  # Connection pool size
                    ttl_dns_cache=300,  # DNS cache TTL
                    use_dns_cache=True,
                    ssl=bot.ssl_context,
                    keepalive_timeout=75.0
                )
                
                # Use orjson for faster serialization
                json_dumps = partial(orjson.dumps, option=orjson.OPT_SERIALIZE_NUMPY)
                
                bot.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    json_serialize=json_dumps
                )
                
                # Configure bot's blockfrost session with optimized settings
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                connector = TCPConnector(
                    limit=100,  # Connection pool size
                    ttl_dns_cache=300,  # DNS cache TTL
                    use_dns_cache=True,
                    ssl=bot.ssl_context,
                    keepalive_timeout=75.0
                )
                
                # Use orjson for faster serialization
                json_dumps = partial(orjson.dumps, option=orjson.OPT_SERIALIZE_NUMPY)
                
                bot.blockfrost_session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    json_serialize=json_dumps
                )
                
                # Add middleware for error handling
                @web.middleware
                async def error_middleware(request: web.Request, handler):
                    try:
                        return await handler(request)
                    except web.HTTPException:
                        raise
                    except aiohttp.ClientError as e:
                        logger.error(f"Client error: {e}", exc_info=True)
                        return web.json_response(
                            {"error": "Service temporarily unavailable"},
                            status=503
                        )
                    except Exception as e:
                        logger.error(f"Unhandled error: {e}", exc_info=True)
                        return web.json_response(
                            {"error": "Internal server error"},
                            status=500
                        )
                
                app.middlewares.append(error_middleware)
                
                # Add request tracking middleware
                @web.middleware
                async def request_tracking_middleware(request: web.Request, handler):
                    # Track request count
                    app['request_count'] = app.get('request_count', 0) + 1
                    
                    # Track request timing
                    start_time = asyncio.get_event_loop().time()
                    try:
                        response = await handler(request)
                        duration = asyncio.get_event_loop().time() - start_time
                        
                        # Update request metrics
                        metrics = app.get('request_metrics', {'latency': [], 'status_codes': {}})
                        metrics['latency'].append(duration)
                        if len(metrics['latency']) > 100:  # Keep last 100 requests
                            metrics['latency'] = metrics['latency'][-100:]
                        
                        metrics['status_codes'][response.status] = metrics['status_codes'].get(response.status, 0) + 1
                        app['request_metrics'] = metrics
                        
                        return response
                    except Exception as e:
                        duration = asyncio.get_event_loop().time() - start_time
                        metrics = app.get('request_metrics', {'latency': [], 'status_codes': {}})
                        metrics['latency'].append(duration)
                        metrics['status_codes'][500] = metrics['status_codes'].get(500, 0) + 1
                        app['request_metrics'] = metrics
                        raise
                
                app.middlewares.append(request_tracking_middleware)
                
                # Add webhook tracking
                app['webhook_metrics'] = {
                    'success_count': 0,
                    'failure_count': 0,
                    'last_success': None,
                    'last_failure': None,
                    'errors': []  # Keep last few errors
                }
                
            except Exception as e:
                logger.error(f"Failed to initialize bot: {e}", exc_info=True)
                raise
        
        # Add routes with error handling
        app.router.add_get('/health', health_check)
        app.router.add_post('/webhook', bot.handle_webhook)
        
        # Add cleanup callback
        app.on_cleanup.append(cleanup)
        
        # Start bot in background
        if not bot.is_closed():
            asyncio.create_task(bot.start(os.getenv('DISCORD_TOKEN')))
        
        return app

async def health_check(request: web.Request) -> web.Response:
    """Health check endpoint that integrates with the bot's health check"""
    try:
        # Get health data from bot
        health_data = await bot.health_check() if bot else {
            'status': 'unhealthy',
            'timestamp': datetime.utcnow().isoformat(),
            'error': 'Bot not initialized',
            'components': {}
        }
        
        # Map status to HTTP code
        status_code = {
            'healthy': 200,
            'degraded': 200,  # Still return 200 for load balancers
            'unhealthy': 503
        }.get(health_data['status'], 503)
        
        # Add WSGI-specific info
        metrics = app.get('request_metrics', {'latency': [], 'status_codes': {}})
        webhook_metrics = app.get('webhook_metrics', {
            'success_count': 0,
            'failure_count': 0
        })
        
        # Calculate latency stats
        latency_stats = {
            'avg': sum(metrics['latency']) / len(metrics['latency']) if metrics['latency'] else 0,
            'max': max(metrics['latency']) if metrics['latency'] else 0,
            'min': min(metrics['latency']) if metrics['latency'] else 0,
            'p95': sorted(metrics['latency'])[int(len(metrics['latency']) * 0.95)] if len(metrics['latency']) > 20 else None
        }
        
        # Get worker memory usage
        try:
            process = psutil.Process()
            worker_memory = process.memory_info().rss / 1024 / 1024  # MB
        except Exception as e:
            logger.warning(f"Failed to get worker memory: {e}")
            worker_memory = None
        
        health_data['components']['wsgi'] = {
            'healthy': True,
            'workers': len([t for t in asyncio.all_tasks()]),
            'request_metrics': {
                'total': app.get('request_count', 0),
                'status_codes': metrics['status_codes'],
                'latency': latency_stats
            },
            'webhook_metrics': {
                'success_rate': (
                    webhook_metrics['success_count'] / 
                    (webhook_metrics['success_count'] + webhook_metrics['failure_count'])
                    if webhook_metrics['success_count'] + webhook_metrics['failure_count'] > 0 
                    else None
                ),
                'total_webhooks': webhook_metrics['success_count'] + webhook_metrics['failure_count'],
                'last_success': webhook_metrics.get('last_success'),
                'last_failure': webhook_metrics.get('last_failure'),
                'recent_errors': webhook_metrics.get('errors', [])[-3:]  # Last 3 errors
            },
            'memory_mb': worker_memory,
            'uptime': str(datetime.utcnow() - app.get('start_time', datetime.utcnow()))
        }
        
        # Return JSON response
        return web.json_response(
            health_data,
            status=status_code,
            dumps=partial(orjson.dumps, option=orjson.OPT_SERIALIZE_NUMPY)
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return web.json_response(
            {
                'status': 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'components': {
                    'wsgi': {
                        'healthy': False,
                        'error': str(e)
                    }
                }
            },
            status=503,
            dumps=partial(orjson.dumps, option=orjson.OPT_SERIALIZE_NUMPY)
        )

async def cleanup(app):
    """Cleanup function to handle graceful shutdown"""
    global bot
    
    logger.info("Starting cleanup process...")
    
    if bot is not None:
        try:
            # Close bot's session first
            if bot.session is not None:
                await bot.session.close()
                logger.info("Bot session closed successfully")
            
            # Close blockfrost session if it exists
            if bot.blockfrost_session is not None:
                await bot.blockfrost_session.close()
                logger.info("Blockfrost session closed successfully")
            
            # Close the bot itself
            await bot.close()
            logger.info("Bot closed successfully")
            
        except Exception as e:
            logger.error(f"Error during bot cleanup: {str(e)}")
        finally:
            bot = None
    
    logger.info("Cleanup completed")

def signal_handler():
    """Handle system signals for graceful shutdown"""
    logger.info("Received shutdown signal")
    if app is not None:
        asyncio.create_task(cleanup(app))

# This is the entry point that Gunicorn calls
async def app_factory():
    """
    Application factory for Gunicorn.
    This is an async function that returns an Application instance,
    which is what aiohttp.GunicornWebWorker expects.
    """
    return await init_app()

# For Gunicorn
app_factory = app_factory

if __name__ == '__main__':
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: signal_handler())
    
    # Run the application with optimized settings
    web.run_app(
        init_app(),  # For local development
        port=int(os.getenv('PORT', 8080)),
        ssl_context=bot.ssl_context if bot else None,
        keepalive_timeout=75.0,
        shutdown_timeout=60.0,
        client_max_size=1024**2 * 50  # 50MB max request size
    )
