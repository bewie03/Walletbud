import os
import re
import ssl
import time
import json
import uuid
import certifi
import asyncio
import logging
import discord
import aiohttp
import asyncpg
import traceback

from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime, timedelta
from ipaddress import ip_network
from aiohttp import web
from discord.ext import commands, tasks
from blockfrost import BlockFrostApi

import os
import logging
import sys
from datetime import datetime
import time
import asyncio
import aiohttp
import requests
import discord
from discord import app_commands
from aiohttp import web
from discord.ext import commands, tasks
from typing import Dict, Any, Optional, List, Union
import asyncpg
import threading
from blockfrost import BlockFrostApi, ApiUrls
from blockfrost.api.cardano.network import network
from database import (
    get_pool,
    add_wallet_for_user,
    remove_wallet_for_user,
    get_user_id_for_stake_address,
    get_user_wallets,
    get_notification_settings,
    update_notification_setting,
    initialize_notification_settings,
    get_wallet_for_user,
    init_db,
    get_all_monitored_addresses,
    get_addresses_for_stake,
    update_pool_for_stake
)
from database_maintenance import DatabaseMaintenance
from webhook_queue import WebhookQueue
from decorators import dm_only, has_blockfrost, command_cooldown
from config import (
    DISCORD_TOKEN,
    ADMIN_CHANNEL_ID,
    BLOCKFROST_PROJECT_ID,
    BLOCKFROST_BASE_URL,
    MAX_REQUESTS_PER_SECOND,
    BURST_LIMIT,
    RATE_LIMIT_COOLDOWN,
    RATE_LIMIT_WINDOW,
    RATE_LIMIT_MAX_REQUESTS,
    MAX_QUEUE_SIZE,
    MAX_RETRIES,
    MAX_EVENT_AGE,
    BATCH_SIZE,
    MAX_WEBHOOK_SIZE,
    WEBHOOK_RATE_LIMIT,
    PROCESS_INTERVAL,
    MAX_ERROR_HISTORY,
    WALLET_CHECK_INTERVAL,
    MIN_ADA_BALANCE,
    MAX_TX_PER_HOUR,
    MINIMUM_YUMMI,
    MAINTENANCE_HOUR,
    MAINTENANCE_MINUTE,
    ASSET_ID,
    YUMMI_POLICY_ID,
    YUMMI_TOKEN_NAME,
    WEBHOOK_IDENTIFIER,
    WEBHOOK_AUTH_TOKEN,
    WEBHOOK_CONFIRMATIONS,
    ERROR_MESSAGES
)

import uuid
import random
import psutil
import functools
from functools import wraps
from collections import defaultdict

import config
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_request_id():
    """Generate a unique request ID for logging"""
    return str(uuid.uuid4())

class RateLimiter:
    """Rate limiter with burst support and per-endpoint tracking"""
    
    def __init__(self, max_requests: int, burst_limit: int, cooldown_seconds: int):
        self.max_requests = max_requests
        self.burst_limit = burst_limit
        self.cooldown_seconds = cooldown_seconds
        self.endpoints = defaultdict(lambda: {
            'tokens': self.max_requests,
            'last_update': time.time(),
            'lock': asyncio.Lock()
        })
        logger.info(f"Initialized RateLimiter with: max_requests={max_requests}, burst_limit={burst_limit}, cooldown_seconds={cooldown_seconds}")

    async def acquire(self, endpoint: str):
        """Acquire a rate limit token for the specified endpoint"""
        async with self.endpoints[endpoint]['lock']:
            # Refresh tokens if cooldown has passed
            current_time = time.time()
            time_passed = current_time - self.endpoints[endpoint]['last_update']
            
            if time_passed >= self.cooldown_seconds:
                self.endpoints[endpoint]['tokens'] = self.max_requests
                self.endpoints[endpoint]['last_update'] = current_time
            
            # Wait if no tokens available
            while self.endpoints[endpoint]['tokens'] <= 0:
                await asyncio.sleep(0.1)
                
                # Refresh tokens if cooldown has passed
                current_time = time.time()
                time_passed = current_time - self.endpoints[endpoint]['last_update']
                
                if time_passed >= self.cooldown_seconds:
                    self.endpoints[endpoint]['tokens'] = self.max_requests
                    self.endpoints[endpoint]['last_update'] = current_time
            
            # Consume a token
            self.endpoints[endpoint]['tokens'] -= 1

    def release(self, endpoint: str):
        """Release a rate limit token back to the specified endpoint"""
        if self.endpoints[endpoint]['tokens'] < self.burst_limit:
            self.endpoints[endpoint]['tokens'] += 1

class WalletBudBot(commands.Bot):
    """WalletBud Discord bot"""
    NOTIFICATION_TYPES = {
        # Command value -> Database key
        "ada_transactions": "ada_transactions",
        "token_changes": "token_changes", 
        "nft_updates": "nft_updates",
        "stake_changes": "stake_changes",
        "policy_expiry": "policy_expiry",
        "delegation_status": "delegation_status",
        "staking_rewards": "staking_rewards",
        "dapp_interactions": "dapp_interactions",
        "failed_transactions": "failed_transactions"
    }
    
    NOTIFICATION_DISPLAY = {
        # Database key -> Display name
        "ada_transactions": "ADA Transactions",
        "token_changes": "Token Changes",
        "nft_updates": "NFT Updates",
        "stake_changes": "Stake Key Changes",
        "policy_expiry": "Policy Expiry Alerts",
        "delegation_status": "Delegation Status",
        "staking_rewards": "Staking Rewards",
        "dapp_interactions": "DApp Interactions",
        "failed_transactions": "Failed Transactions"
    }

    DEFAULT_SETTINGS = {
        "ada_transactions": True,
        "token_changes": True,
        "nft_updates": True,
        "stake_changes": True,
        "policy_expiry": True,
        "delegation_status": True,
        "staking_rewards": True,
        "dapp_interactions": True,
        "failed_transactions": True
    }

    DAPP_IDENTIFIERS = {
        # Common metadata fields
        "metadata_fields": ["dapp", "platform", "protocol", "application"],
        
        # DApp name -> List of identifiers in metadata
        "SundaeSwap": ["sundae", "sundaeswap", "sundaeswap.io", "swap.sundae"],
        "MuesliSwap": ["muesli", "muesliswap", "muesliswap.com", "swap.muesli"],
        "MinSwap": ["min", "minswap", "minswap.org", "swap.min"],
        "WingRiders": ["wing", "wingriders", "wingriders.com"],
        "VyFinance": ["vyfi", "vyfinance", "vyfi.io"],
        "Genius Yield": ["genius", "geniusyield", "geniusyield.co"],
        "Indigo Protocol": ["indigo", "indigoprotocol", "indigoprotocol.io"],
        "Liqwid Finance": ["liqwid", "liqwidfinance", "liqwid.finance"],
        "AADA Finance": ["aada", "aadafinance", "aada.finance"],
        "Djed": ["djed", "shen", "djed.xyz"],
        "Ardana": ["ardana", "ardana.io"],
        "Optim Finance": ["optim", "optimfinance"],
        "Spectrum": ["spectrum", "spectrum.fi"],
        "JPEG Store": ["jpeg", "jpgstore", "jpg.store"],
        "NFT/Token Contract": ["721", "20", "nft", "token"]  # NFT and Token standards
    }

    def __init__(self, *args, **kwargs):
        """Initialize the bot with required intents"""
        intents = discord.Intents.default()
        intents.message_content = True
        
        # Initialize the bot with required parameters
        super().__init__(
            command_prefix='!',  # Required but not used since we use slash commands
            intents=intents,
            application_id=os.getenv('APPLICATION_ID'),
            *args,
            **kwargs
        )
        
        # Get admin channel ID from environment
        self.admin_channel_id = int(os.getenv('ADMIN_CHANNEL_ID', 0))
        self.admin_channel = None  # Will be set in setup_hook
        
        # Initialize components
        self.rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND, BURST_LIMIT, RATE_LIMIT_COOLDOWN)
        self.db_maintenance = DatabaseMaintenance()
        self.blockfrost = None  # Initialize Blockfrost client as None
        self.session = None
        self.connector = None
        
        # Initialize webhook components
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)
        self.site = None
        self._webhook_queue = asyncio.Queue()
        self._webhook_processor = None
        
        # Initialize health metrics
        self.health_metrics = {
            'start_time': None,
            'last_api_call': None,
            'last_db_query': None,
            'last_webhook': None,
            'blockfrost_init': None,
            'webhook_success': 0,
            'webhook_failure': 0,
            'errors': []
        }
        
        # Initialize monitoring state
        self.monitoring_paused = False
        self.yummi_check_lock = asyncio.Lock()
        self.processing_yummi = False
        
        # Pre-compile DApp metadata patterns
        self.dapp_patterns = {
            name.lower(): [pattern.lower() for pattern in patterns]
            for name, patterns in self.DAPP_IDENTIFIERS.items()
            if name != "metadata_fields"
        }
        self.metadata_fields = [field.lower() for field in self.DAPP_IDENTIFIERS["metadata_fields"]]
        
        # Initialize interaction rate limiting
        self.interaction_cooldowns = {}
        self.interaction_lock = asyncio.Lock()
        self.active_interactions = {}
        self.interaction_timeouts = {}
        self.webhook_retries = {}
        
        # Initialize YUMMI check task (every 6 hours)
        self.check_yummi_balances = tasks.loop(hours=6)(self._check_yummi_balances)
        
        # Initialize SSL context with certificate verification enabled
        import ssl
        self.ssl_context = ssl.create_default_context()
        
        # Initialize aiohttp connector with default settings
        self.connector = aiohttp.TCPConnector(ssl=self.ssl_context, limit=100)
        
        # Add command locks
        self.command_locks = {}
        self.health_lock = asyncio.Lock()  # Specific lock for health command

    async def setup_hook(self):
        """Set up the bot's background tasks"""
        try:
            logger.debug("Starting background tasks...")
            
            # Initialize database
            await self.init_database()
            
            # Wait for bot to be ready before setting up admin channel
            await self.wait_until_ready()
            
            # Set up admin channel
            if not await self.setup_admin_channel():
                logger.error("Failed to set up admin channel")
            
            # Initialize Blockfrost
            if not await self.init_blockfrost():
                logger.error("Failed to initialize Blockfrost API")
            
            # Start background tasks
            if hasattr(self, 'pool') and self.pool:
                try:
                    self.check_yummi_balances.start()
                    logger.info("YUMMI balance check task started")
                except Exception as e:
                    logger.error(f"Could not start YUMMI balance check: {e}")
            
            # Start connection check task
            self.check_connection_task = self.loop.create_task(self._check_connection_loop())
            logger.info("Connection check task started")
            
            # Update health metrics
            await self.update_health_metrics('start_time', datetime.now().isoformat())
            logger.info("Bot initialization complete")
            
        except Exception as e:
            logger.error(f"Failed to start background tasks: {str(e)}")
            logger.warning("Continuing without background tasks...")
            
    async def close(self):
        """Clean up resources when the bot is shutting down."""
        try:
            # Cancel background tasks
            if hasattr(self, 'check_connection_task'):
                self.check_connection_task.cancel()
            
            if hasattr(self, 'check_yummi_balances'):
                self.check_yummi_balances.stop()
            
            # Close aiohttp session
            if self.session:
                await self.session.close()
            
            # Close database pool
            if hasattr(self, 'pool') and self.pool:
                await self.pool.close()
            
            # Close connector
            if self.connector:
                await self.connector.close()
            
            # Close webhook components
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()
            
            await super().close()
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    async def init_database(self):
        """Initialize database connection pool"""
        try:
            # Create connection pool
            self.db_pool = await asyncpg.create_pool(
                os.getenv('DATABASE_URL'),
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            
            # Test connection
            async with self.db_pool.acquire() as conn:
                await conn.execute('SELECT 1')
                
            logger.info("Database connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            await self.send_admin_alert(f"Failed to initialize database: {e}")
            return False
            
    async def check_database(self):
        """Check database connection and reconnect if needed"""
        try:
            if not hasattr(self, 'pool') or not self.pool:
                await self.init_database()
                
            # Test the connection with a simple query
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
                
            self.update_health_metrics('last_db_query')
            return None
            
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise  # Re-raise the exception for health check to handle

    async def rate_limited_request(self, func, *args, **kwargs):
        """Execute a rate-limited request to Blockfrost API with improved error handling"""
        max_retries = 3
        base_delay = 1  # Base delay in seconds
        
        for attempt in range(max_retries):
            try:
                if not self.blockfrost:
                    raise ValueError("Blockfrost API client not initialized")
                    
                async with self.rate_limiter.acquire('blockfrost'):
                    response = await func(*args, **kwargs)
                    self.update_health_metrics('last_api_call')
                    return response
                    
            except ApiError as e:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                
                if e.status_code == 400:  # Invalid request
                    logger.error(f"Invalid Blockfrost API request: {str(e)}")
                    raise
                elif e.status_code == 402:  # Project exceeded
                    logger.error("Project quota exceeded")
                    raise
                elif e.status_code == 403:  # Authentication error
                    logger.error("Invalid API key")
                    raise
                elif e.status_code == 429:  # Too many requests
                    if attempt < max_retries - 1:
                        logger.warning(f"Rate limit hit, retrying in {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Rate limit exceeded after retries")
                        raise
                elif e.status_code >= 500:  # Server errors
                    if attempt < max_retries - 1:
                        logger.warning(f"Server error, retrying in {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Server error persisted after retries")
                        raise
                else:
                    logger.error(f"Unexpected API error: {str(e)}")
                    raise
                    
            except Exception as e:
                logger.error(f"Unexpected error in rate_limited_request: {str(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                raise
                
        raise Exception("Max retries exceeded")

    async def send_admin_alert(self, message: str, is_error: bool = True):
        """Send alert to admin channel with enhanced details"""
        try:
            if not self.admin_channel_id:
                return
                
            channel = self.get_channel(int(self.admin_channel_id))
            if not channel:
                logger.error("Admin channel not found")
                return
                
            # Create rich embed for alert
            embed = discord.Embed(
                title="🚨 Error Alert" if is_error else "ℹ️ System Alert",
                description=message,
                color=discord.Color.red() if is_error else discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Add system info
            embed.add_field(
                name="System Status",
                value=f"""
                • Database: {'✅' if await self.check_database() else '❌'}
                • Blockfrost: {'✅' if await self.check_blockfrost() else '❌'}
                • Memory Usage: {psutil.Process().memory_info().rss / 1024 / 1024:.1f} MB
                • CPU Usage: {psutil.cpu_percent()}%
                """.strip(),
                inline=False
            )
            
            # Add error context if available
            import traceback
            tb = traceback.format_exc()
            if tb and tb != "NoneType: None\n":
                embed.add_field(
                    name="Error Details",
                    value=f"```python\n{tb[:1000]}```",
                    inline=False
                )
            
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to send admin alert: {str(e)}")

    async def process_interaction(self, interaction: discord.Interaction, ephemeral: bool = True):
        """Process interaction with proper error handling"""
        try:
            # Defer response immediately
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except discord.errors.NotFound:
            logger.error("Interaction not found")
            return False
        except Exception as e:
            logger.error(f"Error deferring interaction: {str(e)}")
            return False

    async def safe_send(self, interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> bool:
        """Safely send a message through interaction, handling rate limits and errors"""
        interaction_id = str(interaction.id)
        
        # Check if we've already responded to this interaction
        if interaction_id in self.active_interactions:
            logger.debug(f"Already responded to interaction {interaction_id}")
            return False
        
        # Mark interaction as active
        self.active_interactions[interaction_id] = True
        
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content, ephemeral=ephemeral)
            return True
        except discord.errors.InteractionResponded:
            logger.debug(f"Interaction {interaction_id} already responded to")
            return False
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limited
                logger.warning(f"Rate limited on interaction {interaction_id}")
                # Clear interaction from active list so we can retry
                self.active_interactions.pop(interaction_id, None)
                return False
            logger.error(f"HTTP error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            return False
        finally:
            # Clean up interaction tracking after a delay
            asyncio.create_task(self.cleanup_interaction(interaction_id))

    async def cleanup_interaction(self, interaction_id: str):
        """Clean up completed interaction"""
        await asyncio.sleep(5)  # Wait 5 seconds before cleanup
        self.active_interactions.pop(interaction_id, None)
        self.webhook_retries.pop(interaction_id, None)

    async def send_response(self, interaction: discord.Interaction, content=None, embed=None, ephemeral: bool = True):
        """Send response with proper error handling"""
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
            return True
        except Exception as e:
            logger.error(f"Error sending response: {str(e)}")
            return False

    async def send_dm(self, user_id: int, content: str):
        """Send a direct message to a user"""
        try:
            user = await self.fetch_user(user_id)
            if user:
                await user.send(content)
            else:
                logger.error(f"Failed to find user {user_id} for DM")
        except Exception as e:
            logger.error(f"Error sending DM: {str(e)}")

    @app_commands.command(name="health", description="Check system health and status")
    async def health(self, interaction: discord.Interaction):
        """Check system health and status"""
        try:
            # Acquire lock to prevent concurrent health checks
            async with self.health_lock:
                # Create embed for health status
                embed = discord.Embed(
                    title="🏥 System Health Report",
                    description="Current status of bot systems",
                    color=discord.Color.blue()
                )
                
                # Add bot uptime
                if self.health_metrics['start_time']:
                    start_time = datetime.fromisoformat(self.health_metrics['start_time'])
                    uptime = datetime.now() - start_time
                    hours = int(uptime.total_seconds() / 3600)
                    minutes = int((uptime.total_seconds() % 3600) / 60)
                    embed.add_field(
                        name="⏱️ Uptime",
                        value=f"{hours} hours, {minutes} minutes",
                        inline=True
                    )
                
                # Add API status
                api_status = "🟢 Online" if self.blockfrost else "🔴 Offline"
                embed.add_field(
                    name="🌐 Blockfrost API",
                    value=api_status,
                    inline=True
                )
                
                # Add database status
                db_status = "🟢 Connected" if hasattr(self, 'pool') and self.pool else "🔴 Disconnected"
                embed.add_field(
                    name="🗄️ Database",
                    value=db_status,
                    inline=True
                )
                
                # Add webhook metrics
                embed.add_field(
                    name="📊 Webhook Metrics",
                    value=f"Success: {self.health_metrics.get('webhook_success', 0)}\nFailures: {self.health_metrics.get('webhook_failure', 0)}",
                    inline=True
                )
                
                # Add last activity timestamps
                timestamps = []
                if self.health_metrics.get('last_api_call'):
                    timestamps.append(f"API Call: {self.health_metrics['last_api_call']}")
                if self.health_metrics.get('last_db_query'):
                    timestamps.append(f"DB Query: {self.health_metrics['last_db_query']}")
                if self.health_metrics.get('last_webhook'):
                    timestamps.append(f"Webhook: {self.health_metrics['last_webhook']}")
                
                if timestamps:
                    embed.add_field(
                        name="⏰ Last Activity",
                        value="\n".join(timestamps),
                        inline=False
                    )
                
                # Add recent errors if any
                recent_errors = self.health_metrics.get('errors', [])[-5:]  # Show last 5 errors
                if recent_errors:
                    error_text = "\n".join(f"• {error}" for error in recent_errors)
                    embed.add_field(
                        name="❌ Recent Errors",
                        value=error_text[:1024],  # Discord field value limit
                        inline=False
                    )
                
                # Send response
                await interaction.response.send_message(embed=embed, ephemeral=True)
                
        except Exception as e:
            logger.error(f"Error in health command: {str(e)}", exc_info=True)
            await interaction.response.send_message(
                "❌ An error occurred while checking system health. Please try again later.",
                ephemeral=True
            )

    async def on_ready(self):
        """Called when the bot is ready and connected to Discord"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            
            # Set up admin channel
            if self.admin_channel_id:
                if await self.setup_admin_channel():
                    try:
                        await self.admin_channel.send("🟢 Bot is now online and monitoring wallets!")
                    except Exception as e:
                        logger.error(f"Could not send message to admin channel: {e}")
                else:
                    logger.warning("Admin channel setup failed")
            
            # Update status with custom activity
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(status=discord.Status.online, activity=activity)
            logger.info("Bot status updated")
            
            # Initialize Blockfrost
            await self.init_blockfrost()
            
            # Start connection check task
            self.check_connection_task = self.loop.create_task(self._check_connection_loop())
            logger.info("Connection check task started")
            
            # Start background tasks if database is available
            if hasattr(self, 'pool') and self.pool:
                try:
                    self.check_yummi_balances.start()
                    logger.info("YUMMI balance check task started")
                except Exception as e:
                    logger.error(f"Could not start YUMMI balance check: {e}")
            
            # Update health metrics
            await self.update_health_metrics('start_time', datetime.now().isoformat())
            logger.info("Bot initialization complete")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}", exc_info=True)
            if self.admin_channel:
                await self.admin_channel.send(f"⚠️ Error during bot initialization: {str(e)}")

    async def on_connect(self):
        """Called when the bot connects to Discord"""
        logger.info("Bot connected to Discord Gateway")
        try:
            # Set initial presence
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(status=discord.Status.online, activity=activity)
            
            # Initialize session
            if not self.session:
                self.session = aiohttp.ClientSession()
                logger.info("aiohttp session initialized")
                
        except Exception as e:
            logger.error(f"Error in on_connect: {str(e)}", exc_info=True)

    async def on_disconnect(self):
        """Called when the bot disconnects from Discord"""
        logger.warning("Bot disconnected from Discord Gateway")
        try:
            # Log detailed connection info
            logger.error("=== Connection Debug Info ===")
            logger.error(f"Last sequence: {self.ws.sequence if hasattr(self, 'ws') else 'No websocket'}")
            logger.error(f"Latency: {self.latency * 1000:.2f}ms")
            logger.error(f"Is closed: {self.is_closed()}")
            logger.error(f"Is ready: {self.is_ready()}")
            logger.error(f"User: {self.user if hasattr(self, 'user') else 'No user'}")
            
            # Try to reconnect if not shutting down
            if not self.is_closed():
                logger.info("Attempting to reconnect...")
                try:
                    # Update presence to show reconnecting status
                    activity = discord.Activity(
                        type=discord.ActivityType.watching,
                        name="Reconnecting..."
                    )
                    await self.change_presence(status=discord.Status.idle, activity=activity)
                except Exception as e:
                    logger.error(f"Failed to update presence: {e}")

        except Exception as e:
            logger.error(f"Error in on_disconnect: {str(e)}", exc_info=True)

    async def on_error(self, event_method: str, *args, **kwargs):
        """Called when an error occurs in an event"""
        logger.error(f"=== Error in {event_method} ===")
        logger.error(f"Args: {args}")
        logger.error(f"Kwargs: {kwargs}")
        import traceback
        traceback.print_exc()

    async def check_connections(self):
        """Check all connections and log their status"""
        try:
            # Check Discord connection
            if self.is_ready():
                logger.info("✅ Discord connection is active")
            else:
                logger.error("❌ Discord connection is not ready")
                return False

            # Check Blockfrost connection if available
            if hasattr(self, 'blockfrost') and self.blockfrost:
                try:
                    health = await self.blockfrost.health()
                    logger.info(f"✅ Blockfrost connection is active. Health: {health}")
                except Exception as e:
                    logger.error(f"❌ Blockfrost connection failed: {e}")
                    if hasattr(e, 'status_code'):
                        logger.error(f"Status code: {e.status_code}")
                    return False
            else:
                logger.warning("⚠️ Blockfrost client not initialized")
            
            # Check database connection if available
            if hasattr(self, 'pool') and self.pool:
                try:
                    async with self.pool.acquire() as conn:
                        await conn.execute('SELECT 1')
                    logger.info("✅ Database connection is active")
                except Exception as e:
                    logger.error(f"❌ Database connection failed: {e}")
                    return False
            else:
                logger.warning("⚠️ Database pool not initialized")

            return True
            
        except Exception as e:
            logger.error(f"Error checking connections: {e}")
            return False
    
    async def setup_admin_channel(self):
        """Set up admin channel for bot notifications"""
        try:
            if not self.admin_channel_id:
                logger.warning("No admin channel ID configured")
                return False
            
            # Wait for bot to be ready
            if not self.is_ready():
                logger.info("Waiting for bot to be ready...")
                await self.wait_until_ready()
            
            # Get the channel
            channel = self.get_channel(self.admin_channel_id)
            if not channel:
                logger.error(f"Could not find channel with ID {self.admin_channel_id}")
                return False
            
            # Test permissions by sending a message
            try:
                await channel.send("🔄 Testing admin channel permissions...")
                self.admin_channel = channel
                logger.info(f"Admin channel set up successfully: {channel.name}")
                return True
            except Exception as e:
                logger.error(f"Could not send message to admin channel: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting up admin channel: {e}")
            return False
    
    async def _check_connection_loop(self):
        """Background task to periodically check connection status"""
        try:
            while not self.is_closed():
                try:
                    # Wait for bot to be ready
                    if not self.is_ready():
                        await asyncio.sleep(5)
                        continue
                    
                    # Check connections
                    if not await self.check_connections():
                        logger.warning("Connection check failed!")
                        if self.admin_channel:
                            await self.admin_channel.send("⚠️ Connection check failed! Check logs for details.")
                    
                    # Update health metrics
                    await self.update_health_metrics('last_connection_check', datetime.now().isoformat())
                    
                except Exception as e:
                    logger.error(f"Error in connection check loop: {e}")
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
        except asyncio.CancelledError:
            logger.info("Connection check task cancelled")
        except Exception as e:
            logger.error(f"Connection check task failed: {e}")

    async def on_resumed(self):
        """Called when the bot resumes a session"""
        logger.info("Session resumed")
        try:
            # Update presence
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(status=discord.Status.online, activity=activity)
            
            # Notify admin channel
            if self.admin_channel:
                await self.admin_channel.send("🟢 Bot connection resumed!")
                
        except Exception as e:
            logger.error(f"Error in on_resumed: {str(e)}", exc_info=True)

    async def check_webhook_rate_limit(self, ip: str) -> bool:
        """Check if webhook request exceeds rate limit
        
        Args:
            ip (str): IP address of the request
            
        Returns:
            bool: True if allowed, False if rate limited
        """
        async with self.webhook_lock:
            now = time.time()
            minute_ago = now - 60
            
            # Clean old entries
            self.webhook_counts = {
                k: (count, ts) for k, (count, ts) in self.webhook_counts.items()
                if ts > minute_ago
            }
            
            # Check current IP
            if ip in self.webhook_counts:
                count, _ = self.webhook_counts[ip]
                if count >= WEBHOOK_RATE_LIMIT:
                    logger.warning(f"Rate limit exceeded for IP {ip}")
                    return False
                self.webhook_counts[ip] = (count + 1, now)
            else:
                self.webhook_counts[ip] = (1, now)
            
            return True

    async def handle_webhook_with_retry(self, request: web.Request):
        """Handle webhook with exponential backoff retry"""
        max_retries = 5
        base_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
                return await self.handle_webhook(request)
            except discord.errors.HTTPException as e:
                if e.status == 429:  # Rate limit error
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else base_delay * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {retry_after}s before retry (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(retry_after)
                else:
                    raise
            except Exception as e:
                logger.error(f"Error processing webhook (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(base_delay * (2 ** attempt))
        
        raise Exception("Max retries exceeded")

    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhooks from Blockfrost with comprehensive validation"""
        request_id = get_request_id()
        client_ip = request.remote
        
        try:
            # 1. IP Whitelist Check
            if BLOCKFROST_IP_RANGES and not any(
                ip_network(range).supernet_of(ip_network(client_ip))
                for range in BLOCKFROST_IP_RANGES
            ):
                logger.warning(f"[{request_id}] Rejected webhook from unauthorized IP: {client_ip}")
                return web.Response(status=403, text="Unauthorized IP")
            
            # 2. Rate Limit Check
            if not self.check_webhook_rate_limit(client_ip):
                logger.warning(f"[{request_id}] Rate limit exceeded for IP: {client_ip}")
                return web.Response(status=429, text="Rate limit exceeded")
            
            # 3. Size Check
            if request.content_length and request.content_length > MAX_WEBHOOK_SIZE:
                logger.warning(f"[{request_id}] Webhook payload too large: {request.content_length} bytes")
                return web.Response(status=413, text="Payload too large")
            
            # 4. Required Headers Check
            webhook_id = request.headers.get('Webhook-Id')
            signature = request.headers.get('Signature')
            if not all([webhook_id, signature]):
                logger.warning(f"[{request_id}] Missing required headers")
                return web.Response(status=400, text="Missing required headers")
            
            # 5. Payload Validation
            try:
                payload = await request.json()
            except Exception as e:
                logger.warning(f"[{request_id}] Invalid JSON payload: {str(e)}")
                return web.Response(status=400, text="Invalid JSON payload")
            
            # 6. Signature Verification
            if not self.verify_webhook_signature(payload, signature):
                logger.warning(f"[{request_id}] Invalid webhook signature")
                return web.Response(status=401, text="Invalid signature")
            
            # 7. Queue the webhook for processing
            try:
                await self._webhook_queue.put({
                    'id': request_id,
                    'payload': payload,
                    'headers': dict(request.headers),
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"[{request_id}] Webhook queued successfully")
                return web.Response(status=202, text="Accepted")
                
            except asyncio.QueueFull:
                logger.error(f"[{request_id}] Webhook queue is full")
                return web.Response(status=503, text="Queue is full")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error processing webhook: {str(e)}")
            return web.Response(status=500, text="Internal server error")

    async def _process_webhook_queue(self):
        """Process webhooks from queue"""
        while True:
            try:
                event_type, payload, headers = await self._webhook_queue.get()
                
                try:
                    await self._process_webhook_event(event_type, payload, headers)
                except discord.errors.HTTPException as e:
                    if e.status == 429:  # Rate limit error
                        # Put the item back in queue with exponential backoff
                        retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
                        logger.warning(f"Rate limited in webhook queue, retrying in {retry_after}s")
                        await asyncio.sleep(retry_after)
                        await self._webhook_queue.put((event_type, payload, headers))
                    else:
                        logger.error(f"Discord API error processing webhook: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing webhook from queue: {str(e)}")
                finally:
                    self._webhook_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in webhook queue processor: {str(e)}")
                await asyncio.sleep(5)  # Brief pause before continuing

    async def _process_webhook_event(self, event_type: str, payload: Dict[str, Any], headers: Dict[str, str]):
        """Process a single webhook event with sanitized logging"""
        try:
            # Sanitize headers for logging
            safe_headers = {
                k: "[REDACTED]" if k.lower() in ['authorization', 'signature', 'auth-token'] 
                else v for k, v in headers.items()
            }
            
            self.log_sanitized('info', 
                f"Processing {event_type} webhook",
                headers=safe_headers,
                payload_size=len(str(payload))
            )
            
            # Process based on event type
            if event_type == 'transaction':
                await self._handle_transaction_webhook(payload)
            elif event_type == 'delegation':
                await self._handle_delegation_webhook(payload)
            else:
                logger.warning(f"Unknown webhook event type: {event_type}")
            
            # Update success metrics
            self.update_health_metrics('webhook_success', 
                self.health_metrics.get('webhook_success', 0) + 1
            )
            
        except Exception as e:
            # Update failure metrics
            self.update_health_metrics('webhook_failure',
                self.health_metrics.get('webhook_failure', 0) + 1
            )
            
            # Add error to history (limit to last 100 errors)
            errors = self.health_metrics.get('errors', [])
            errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': type(e).__name__,
                'message': str(e)
            })
            self.health_metrics['errors'] = errors[-100:]
            
            # Log the error
            logger.error(f"Error processing webhook: {str(e)}")
            await self.send_admin_alert(f"Webhook processing error: {str(e)}")

    async def update_health_metrics(self, metric: str, value: Any = None):
        """Update bot health metrics with sanitized logging"""
        try:
            if metric not in self.health_metrics:
                logger.warning(f"Attempted to update unknown metric: {metric}")
                return
                
            if value is None:
                value = datetime.now()
                
            self.health_metrics[metric] = value
            
            # Don't log sensitive values
            safe_value = "[REDACTED]" if "token" in metric.lower() else str(value)
            logger.debug(f"Updated health metric {metric}: {safe_value}")
            
        except Exception as e:
            logger.error(f"Error updating health metrics: {str(e)}")

    def log_sanitized(self, level: str, message: str, **kwargs):
        """Log messages with sensitive data removed"""
        # Fields that might contain sensitive data
        sensitive_fields = [
            'auth_token', 'project_id', 'api_key', 'password',
            'secret', 'token', 'signature', 'private_key'
        ]
        
        # Sanitize kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if any(field in key.lower() for field in sensitive_fields):
                safe_kwargs[key] = "[REDACTED]"
            else:
                safe_kwargs[key] = value
        
        # Sanitize message
        for field in sensitive_fields:
            if field in message.lower():
                pattern = rf'{field}["\']?\s*[:=]\s*["\']?[\w\-\.]+["\']?'
                message = re.sub(pattern, f'{field}=[REDACTED]', message, flags=re.IGNORECASE)
        
        # Get logger method
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message, **safe_kwargs)

    async def init_blockfrost(self):
        """Initialize Blockfrost API client with proper error handling"""
        try:
            project_id = os.getenv('BLOCKFROST_PROJECT_ID')
            base_url = os.getenv('BLOCKFROST_BASE_URL')
            
            if not project_id:
                raise ValueError("BLOCKFROST_PROJECT_ID not set in environment variables")
            
            if not base_url:
                raise ValueError("BLOCKFROST_BASE_URL not set in environment variables")
            
            # Check if we're in development or production
            is_production = os.getenv('ENVIRONMENT', 'development') == 'production'
            
            if is_production:
                # In production (Heroku), use default SSL settings
                self.blockfrost = BlockFrostApi(
                    project_id=project_id,
                    base_url=base_url
                )
            else:
                # In development, create a custom session with SSL verification disabled
                # NOTE: This is only for local development testing
                connector = aiohttp.TCPConnector(ssl=False)
                session = aiohttp.ClientSession(connector=connector)
                self.blockfrost = BlockFrostApi(
                    project_id=project_id,
                    base_url=base_url,
                    session=session
                )
            
            # Test connection with error handling
            try:
                health = await self.blockfrost.health()
                logger.info(f"✅ Blockfrost API initialized successfully. Health: {health}")
                await self.update_health_metrics('blockfrost_init', True)
                return True
            except Exception as e:
                logger.error(f"❌ Failed to test Blockfrost connection: {str(e)}")
                if hasattr(e, 'status_code'):
                    logger.error(f"Status code: {e.status_code}")
                if hasattr(e, 'response'):
                    logger.error(f"Response: {e.response}")
                raise
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Blockfrost API: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            self.blockfrost = None
            await self.update_health_metrics('blockfrost_init', False)
            if hasattr(self, 'admin_channel') and self.admin_channel:
                await self.send_admin_alert("Failed to initialize Blockfrost API")
            return False

    async def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if we should send a notification to the user"""
        try:
            # Get database connection
            pool = await get_pool()
            if not pool:
                logger.error("Failed to get database pool")
                return False
                
            # Use database should_notify function
            return await should_notify(str(user_id), notification_type)
            
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            return False  # Default to not notifying on error

    async def process_interaction(self, interaction: discord.Interaction, ephemeral: bool = True):
        """Process interaction with proper error handling"""
        try:
            # Defer response immediately
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except discord.errors.NotFound:
            logger.error("Interaction not found")
            return False
        except Exception as e:
            logger.error(f"Error deferring interaction: {str(e)}")
            return False

    async def safe_send(self, interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> bool:
        """Safely send a message through interaction, handling rate limits and errors"""
        interaction_id = str(interaction.id)
        
        # Check if we've already responded to this interaction
        if interaction_id in self.active_interactions:
            logger.debug(f"Already responded to interaction {interaction_id}")
            return False
        
        # Mark interaction as active
        self.active_interactions[interaction_id] = True
        
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content, ephemeral=ephemeral)
            return True
        except discord.errors.InteractionResponded:
            logger.debug(f"Interaction {interaction_id} already responded to")
            return False
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limited
                logger.warning(f"Rate limited on interaction {interaction_id}")
                # Clear interaction from active list so we can retry
                self.active_interactions.pop(interaction_id, None)
                return False
            logger.error(f"HTTP error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            return False
        finally:
            # Clean up interaction tracking after a delay
            asyncio.create_task(self.cleanup_interaction(interaction_id))

    async def cleanup_interaction(self, interaction_id: str):
        """Clean up completed interaction"""
        await asyncio.sleep(5)  # Wait 5 seconds before cleanup
        self.active_interactions.pop(interaction_id, None)
        self.webhook_retries.pop(interaction_id, None)

    async def send_response(self, interaction: discord.Interaction, content=None, embed=None, ephemeral: bool = True):
        """Send response with proper error handling"""
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
            return True
        except Exception as e:
            logger.error(f"Error sending response: {str(e)}")
            return False

    async def send_dm(self, user_id: int, content: str):
        """Send a direct message to a user"""
        try:
            user = await self.fetch_user(user_id)
            if user:
                await user.send(content)
            else:
                logger.error(f"Failed to find user {user_id} for DM")
        except Exception as e:
            logger.error(f"Error sending DM: {str(e)}")

    async def setup_admin_channel(self):
        """Set up admin channel for bot notifications"""
        try:
            if not self.admin_channel_id:
                logger.warning("No admin channel ID configured")
                return False
            
            # Wait for bot to be ready
            if not self.is_ready():
                logger.info("Waiting for bot to be ready...")
                await self.wait_until_ready()
            
            # Get the channel
            channel = self.get_channel(self.admin_channel_id)
            if not channel:
                logger.error(f"Could not find channel with ID {self.admin_channel_id}")
                return False
            
            # Test permissions by sending a message
            try:
                await channel.send("🔄 Testing admin channel permissions...")
                self.admin_channel = channel
                logger.info(f"Admin channel set up successfully: {channel.name}")
                return True
            except Exception as e:
                logger.error(f"Could not send message to admin channel: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting up admin channel: {e}")
            return False
    
    async def check_connection(self):
        """Check if the bot is still connected to Discord"""
        try:
            if not self.is_ready():
                logger.error("Bot is not ready!")
                return False
            
            # Check Discord connection
            if not self.latency:
                logger.error("No connection to Discord!")
                return False
            
            # Check Blockfrost connection if available
            if self.blockfrost:
                try:
                    await self.rate_limited_request(self.blockfrost.health)
                    logger.debug("Blockfrost connection OK")
                except Exception as e:
                    logger.error(f"Blockfrost connection failed: {e}")
                    return False
            else:
                logger.warning("Blockfrost client not initialized")
            
            # Check database connection if available
            if hasattr(self, 'pool') and self.pool:
                try:
                    async with self.pool.acquire() as conn:
                        await conn.execute('SELECT 1')
                    logger.debug("Database connection OK")
                except Exception as e:
                    logger.error(f"Database connection failed: {e}")
                    return False
            
            logger.debug("All connections OK")
            return True
            
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False
    
    async def _check_connection_loop(self):
        """Background task to periodically check connection status"""
        try:
            while not self.is_closed():
                try:
                    # Wait for bot to be ready
                    if not self.is_ready():
                        await asyncio.sleep(5)
                        continue
                    
                    # Check connections
                    if not await self.check_connections():
                        logger.warning("Connection check failed!")
                        if self.admin_channel:
                            await self.admin_channel.send("⚠️ Connection check failed! Check logs for details.")
                    
                    # Update health metrics
                    await self.update_health_metrics('last_connection_check', datetime.now().isoformat())
                    
                except Exception as e:
                    logger.error(f"Error in connection check loop: {e}")
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
        except asyncio.CancelledError:
            logger.info("Connection check task cancelled")
        except Exception as e:
            logger.error(f"Connection check task failed: {e}")

    async def on_resumed(self):
        """Called when the bot resumes a session"""
        logger.info("Session resumed")
        try:
            # Update presence
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(status=discord.Status.online, activity=activity)
            
            # Notify admin channel
            if self.admin_channel:
                await self.admin_channel.send("🟢 Bot connection resumed!")
                
        except Exception as e:
            logger.error(f"Error in on_resumed: {str(e)}", exc_info=True)

    async def check_webhook_rate_limit(self, ip: str) -> bool:
        """Check if webhook request exceeds rate limit
        
        Args:
            ip (str): IP address of the request
            
        Returns:
            bool: True if allowed, False if rate limited
        """
        async with self.webhook_lock:
            now = time.time()
            minute_ago = now - 60
            
            # Clean old entries
            self.webhook_counts = {
                k: (count, ts) for k, (count, ts) in self.webhook_counts.items()
                if ts > minute_ago
            }
            
            # Check current IP
            if ip in self.webhook_counts:
                count, _ = self.webhook_counts[ip]
                if count >= WEBHOOK_RATE_LIMIT:
                    logger.warning(f"Rate limit exceeded for IP {ip}")
                    return False
                self.webhook_counts[ip] = (count + 1, now)
            else:
                self.webhook_counts[ip] = (1, now)
            
            return True

    async def handle_webhook_with_retry(self, request: web.Request):
        """Handle webhook with exponential backoff retry"""
        max_retries = 5
        base_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
                return await self.handle_webhook(request)
            except discord.errors.HTTPException as e:
                if e.status == 429:  # Rate limit error
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else base_delay * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {retry_after}s before retry (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(retry_after)
                else:
                    raise
            except Exception as e:
                logger.error(f"Error processing webhook (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(base_delay * (2 ** attempt))
        
        raise Exception("Max retries exceeded")

    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhooks from Blockfrost with comprehensive validation"""
        request_id = get_request_id()
        client_ip = request.remote
        
        try:
            # 1. IP Whitelist Check
            if BLOCKFROST_IP_RANGES and not any(
                ip_network(range).supernet_of(ip_network(client_ip))
                for range in BLOCKFROST_IP_RANGES
            ):
                logger.warning(f"[{request_id}] Rejected webhook from unauthorized IP: {client_ip}")
                return web.Response(status=403, text="Unauthorized IP")
            
            # 2. Rate Limit Check
            if not self.check_webhook_rate_limit(client_ip):
                logger.warning(f"[{request_id}] Rate limit exceeded for IP: {client_ip}")
                return web.Response(status=429, text="Rate limit exceeded")
            
            # 3. Size Check
            if request.content_length and request.content_length > MAX_WEBHOOK_SIZE:
                logger.warning(f"[{request_id}] Webhook payload too large: {request.content_length} bytes")
                return web.Response(status=413, text="Payload too large")
            
            # 4. Required Headers Check
            webhook_id = request.headers.get('Webhook-Id')
            signature = request.headers.get('Signature')
            if not all([webhook_id, signature]):
                logger.warning(f"[{request_id}] Missing required headers")
                return web.Response(status=400, text="Missing required headers")
            
            # 5. Payload Validation
            try:
                payload = await request.json()
            except Exception as e:
                logger.warning(f"[{request_id}] Invalid JSON payload: {str(e)}")
                return web.Response(status=400, text="Invalid JSON payload")
            
            # 6. Signature Verification
            if not self.verify_webhook_signature(payload, signature):
                logger.warning(f"[{request_id}] Invalid webhook signature")
                return web.Response(status=401, text="Invalid signature")
            
            # 7. Queue the webhook for processing
            try:
                await self._webhook_queue.put({
                    'id': request_id,
                    'payload': payload,
                    'headers': dict(request.headers),
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"[{request_id}] Webhook queued successfully")
                return web.Response(status=202, text="Accepted")
                
            except asyncio.QueueFull:
                logger.error(f"[{request_id}] Webhook queue is full")
                return web.Response(status=503, text="Queue is full")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error processing webhook: {str(e)}")
            return web.Response(status=500, text="Internal server error")

    async def _process_webhook_queue(self):
        """Process webhooks from queue"""
        while True:
            try:
                event_type, payload, headers = await self._webhook_queue.get()
                
                try:
                    await self._process_webhook_event(event_type, payload, headers)
                except discord.errors.HTTPException as e:
                    if e.status == 429:  # Rate limit error
                        # Put the item back in queue with exponential backoff
                        retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
                        logger.warning(f"Rate limited in webhook queue, retrying in {retry_after}s")
                        await asyncio.sleep(retry_after)
                        await self._webhook_queue.put((event_type, payload, headers))
                    else:
                        logger.error(f"Discord API error processing webhook: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing webhook from queue: {str(e)}")
                finally:
                    self._webhook_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in webhook queue processor: {str(e)}")
                await asyncio.sleep(5)  # Brief pause before continuing

    async def _process_webhook_event(self, event_type: str, payload: Dict[str, Any], headers: Dict[str, str]):
        """Process a single webhook event with sanitized logging"""
        try:
            # Sanitize headers for logging
            safe_headers = {
                k: "[REDACTED]" if k.lower() in ['authorization', 'signature', 'auth-token'] 
                else v for k, v in headers.items()
            }
            
            self.log_sanitized('info', 
                f"Processing {event_type} webhook",
                headers=safe_headers,
                payload_size=len(str(payload))
            )
            
            # Process based on event type
            if event_type == 'transaction':
                await self._handle_transaction_webhook(payload)
            elif event_type == 'delegation':
                await self._handle_delegation_webhook(payload)
            else:
                logger.warning(f"Unknown webhook event type: {event_type}")
            
            # Update success metrics
            self.update_health_metrics('webhook_success', 
                self.health_metrics.get('webhook_success', 0) + 1
            )
            
        except Exception as e:
            # Update failure metrics
            self.update_health_metrics('webhook_failure',
                self.health_metrics.get('webhook_failure', 0) + 1
            )
            
            # Add error to history (limit to last 100 errors)
            errors = self.health_metrics.get('errors', [])
            errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': type(e).__name__,
                'message': str(e)
            })
            self.health_metrics['errors'] = errors[-100:]
            
            # Log the error
            logger.error(f"Error processing webhook: {str(e)}")
            await self.send_admin_alert(f"Webhook processing error: {str(e)}")

    async def update_health_metrics(self, metric: str, value: Any = None):
        """Update bot health metrics with sanitized logging"""
        try:
            if metric not in self.health_metrics:
                logger.warning(f"Attempted to update unknown metric: {metric}")
                return
                
            if value is None:
                value = datetime.now()
                
            self.health_metrics[metric] = value
            
            # Don't log sensitive values
            safe_value = "[REDACTED]" if "token" in metric.lower() else str(value)
            logger.debug(f"Updated health metric {metric}: {safe_value}")
            
        except Exception as e:
            logger.error(f"Error updating health metrics: {str(e)}")

    def log_sanitized(self, level: str, message: str, **kwargs):
        """Log messages with sensitive data removed"""
        # Fields that might contain sensitive data
        sensitive_fields = [
            'auth_token', 'project_id', 'api_key', 'password',
            'secret', 'token', 'signature', 'private_key'
        ]
        
        # Sanitize kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if any(field in key.lower() for field in sensitive_fields):
                safe_kwargs[key] = "[REDACTED]"
            else:
                safe_kwargs[key] = value
        
        # Sanitize message
        for field in sensitive_fields:
            if field in message.lower():
                pattern = rf'{field}["\']?\s*[:=]\s*["\']?[\w\-\.]+["\']?'
                message = re.sub(pattern, f'{field}=[REDACTED]', message, flags=re.IGNORECASE)
        
        # Get logger method
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message, **safe_kwargs)

    async def init_blockfrost(self):
        """Initialize Blockfrost API client with proper error handling"""
        try:
            project_id = os.getenv('BLOCKFROST_PROJECT_ID')
            base_url = os.getenv('BLOCKFROST_BASE_URL')
            
            if not project_id:
                raise ValueError("BLOCKFROST_PROJECT_ID not set in environment variables")
            
            if not base_url:
                raise ValueError("BLOCKFROST_BASE_URL not set in environment variables")
            
            # Check if we're in development or production
            is_production = os.getenv('ENVIRONMENT', 'development') == 'production'
            
            if is_production:
                # In production (Heroku), use default SSL settings
                self.blockfrost = BlockFrostApi(
                    project_id=project_id,
                    base_url=base_url
                )
            else:
                # In development, create a custom session with SSL verification disabled
                # NOTE: This is only for local development testing
                connector = aiohttp.TCPConnector(ssl=False)
                session = aiohttp.ClientSession(connector=connector)
                self.blockfrost = BlockFrostApi(
                    project_id=project_id,
                    base_url=base_url,
                    session=session
                )
            
            # Test connection with error handling
            try:
                health = await self.blockfrost.health()
                logger.info(f"✅ Blockfrost API initialized successfully. Health: {health}")
                await self.update_health_metrics('blockfrost_init', True)
                return True
            except Exception as e:
                logger.error(f"❌ Failed to test Blockfrost connection: {str(e)}")
                if hasattr(e, 'status_code'):
                    logger.error(f"Status code: {e.status_code}")
                if hasattr(e, 'response'):
                    logger.error(f"Response: {e.response}")
                raise
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Blockfrost API: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            self.blockfrost = None
            await self.update_health_metrics('blockfrost_init', False)
            if hasattr(self, 'admin_channel') and self.admin_channel:
                await self.send_admin_alert("Failed to initialize Blockfrost API")
            return False

    async def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if we should send a notification to the user"""
        try:
            # Get database connection
            pool = await get_pool()
            if not pool:
                logger.error("Failed to get database pool")
                return False
                
            # Use database should_notify function
            return await should_notify(str(user_id), notification_type)
            
        except Exception as e:
            logger.error(f"Error checking notification settings: {str(e)}")
            return False  # Default to not notifying on error

    async def process_interaction(self, interaction: discord.Interaction, ephemeral: bool = True):
        """Process interaction with proper error handling"""
        try:
            # Defer response immediately
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except discord.errors.NotFound:
            logger.error("Interaction not found")
            return False
        except Exception as e:
            logger.error(f"Error deferring interaction: {str(e)}")
            return False

    async def safe_send(self, interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> bool:
        """Safely send a message through interaction, handling rate limits and errors"""
        interaction_id = str(interaction.id)
        
        # Check if we've already responded to this interaction
        if interaction_id in self.active_interactions:
            logger.debug(f"Already responded to interaction {interaction_id}")
            return False
        
        # Mark interaction as active
        self.active_interactions[interaction_id] = True
        
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content, ephemeral=ephemeral)
            return True
        except discord.errors.InteractionResponded:
            logger.debug(f"Interaction {interaction_id} already responded to")
            return False
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limited
                logger.warning(f"Rate limited on interaction {interaction_id}")
                # Clear interaction from active list so we can retry
                self.active_interactions.pop(interaction_id, None)
                return False
            logger.error(f"HTTP error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            return False
        finally:
            # Clean up interaction tracking after a delay
            asyncio.create_task(self.cleanup_interaction(interaction_id))

    async def cleanup_interaction(self, interaction_id: str):
        """Clean up completed interaction"""
        await asyncio.sleep(5)  # Wait 5 seconds before cleanup
        self.active_interactions.pop(interaction_id, None)
        self.webhook_retries.pop(interaction_id, None)

    async def send_response(self, interaction: discord.Interaction, content=None, embed=None, ephemeral: bool = True):
        """Send response with proper error handling"""
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
            return True
        except Exception as e:
            logger.error(f"Error sending response: {str(e)}")
            return False

    async def send_dm(self, user_id: int, content: str):
        """Send a direct message to a user"""
        try:
            user = await self.fetch_user(user_id)
            if user:
                await user.send(content)
            else:
                logger.error(f"Failed to find user {user_id} for DM")
        except Exception as e:
            logger.error(f"Error sending DM: {str(e)}")

    async def _check_yummi_balances(self):
        """Check YUMMI token balances for all monitored wallets"""
        if self.processing_yummi:
            logger.warning("YUMMI balance check already in progress")
            return
            
        async with self.yummi_check_lock:
            try:
                self.processing_yummi = True
                logger.info("Starting YUMMI balance check...")
                
                # Get all monitored wallets
                query = """
                    SELECT DISTINCT user_id, address 
                    FROM monitored_wallets
                    WHERE active = true
                """
                
                async with self.pool.acquire() as conn:
                    wallets = await conn.fetch(query)
                    
                for wallet in wallets:
                    user_id = wallet['user_id']
                    address = wallet['address']
                    
                    # Check YUMMI requirement
                    if not await self.check_yummi_requirement(address, user_id):
                        logger.warning(f"Wallet {address} no longer meets YUMMI requirement")
                        
                        # Send DM to user
                        try:
                            user = await self.fetch_user(user_id)
                            if user:
                                await user.send(
                                    f"⚠️ Your wallet `{address}` no longer meets the minimum YUMMI token requirement. "
                                    f"Monitoring has been disabled. Please ensure you have at least {MINIMUM_YUMMI} YUMMI tokens "
                                    f"and use `/monitor` to re-enable monitoring."
                                )
                        except Exception as e:
                            logger.error(f"Failed to notify user {user_id} about YUMMI requirement: {e}")
                            
                        # Update database
                        update_query = """
                            UPDATE monitored_wallets 
                            SET active = false, updated_at = NOW()
                            WHERE user_id = $1 AND address = $2
                        """
                        async with self.pool.acquire() as conn:
                            await conn.execute(update_query, user_id, address)
                            
                logger.info("YUMMI balance check completed")
                
            except Exception as e:
                logger.error(f"Error checking YUMMI balances: {e}")
                await self.send_admin_alert(f"Error checking YUMMI balances: {e}")
                
            finally:
                self.processing_yummi = False

    def check_environment(self):
        """Check if all required environment variables are set"""
        required_vars = {
            'DISCORD_TOKEN': os.getenv('DISCORD_TOKEN'),
            'APPLICATION_ID': os.getenv('APPLICATION_ID'),
            'ADMIN_CHANNEL_ID': os.getenv('ADMIN_CHANNEL_ID'),
            'BLOCKFROST_PROJECT_ID': os.getenv('BLOCKFROST_PROJECT_ID'),
            'BLOCKFROST_BASE_URL': os.getenv('BLOCKFROST_BASE_URL'),
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        return True

    async def check_blockfrost(self):
        """Check if Blockfrost API is working"""
        try:
            if not self.blockfrost:
                raise ValueError("Blockfrost API client not initialized")
                
            async with self.rate_limiter.acquire('blockfrost'):
                await self.blockfrost.health()
            logger.info("✅ Blockfrost API check successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Blockfrost API check failed: {str(e)}")
            return False

    async def blockfrost_request(self, method: Callable, *args, **kwargs) -> Any:
        """Execute a Blockfrost API request with rate limiting and error handling
        
        Args:
            method (Callable): Blockfrost API method to call
            *args: Arguments to pass to the method
            **kwargs: Keyword arguments to pass to the method
            
        Returns:
            Any: Response from the Blockfrost API
            
        Raises:
            ValueError: If Blockfrost client is not initialized
            Exception: If API call fails
        """
        if not self.blockfrost:
            raise ValueError("Blockfrost API client not initialized")
            
        try:
            async with self.rate_limiter.acquire('blockfrost'):
                return await method(*args, **kwargs)
                
        except Exception as e:
            logger.error(f"Blockfrost API request failed: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise

if __name__ == "__main__":
    try:
        # Configure logging for production
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('bot.log')
            ]
        )
        
        # Configure event loop policy for Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        # Create and set event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create bot instance
        bot = WalletBudBot()
        
        # Check environment variables
        bot.check_environment()
        
        # Run the bot
        bot.run(os.getenv('DISCORD_TOKEN'))
        
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}", exc_info=True)
        sys.exit(1)
