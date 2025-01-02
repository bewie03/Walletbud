import os
import re
import ssl
import sys
import json
import time
import uuid
import signal
import psutil
import asyncio
import logging
import certifi
import discord
import aiohttp
from aiohttp import web, ClientSession
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Coroutine
from discord.ext import commands, tasks
from discord import app_commands
from collections import defaultdict
from cachetools import TTLCache

# Import configuration
from config import (
    DISCORD_TOKEN, APPLICATION_ID, ADMIN_CHANNEL_ID,
    BLOCKFROST_PROJECT_ID, BLOCKFROST_BASE_URL, DATABASE_URL, WEBHOOK_SECRET,
    MAX_REQUESTS_PER_SECOND, BURST_LIMIT, RATE_LIMIT_COOLDOWN,
    YUMMI_POLICY_ID, HEALTH_METRICS_TTL,
    validate_config
)
from shutdown_manager import ShutdownManager

# Third-party imports
import asyncpg
import requests
import psutil
from blockfrost import BlockFrostApi, ApiUrls
from blockfrost.api.cardano.network import network
from urllib3.exceptions import InsecureRequestWarning

# Local imports
from database import (
    # Core database functions
    get_pool,
    init_db,
    get_database_url,
    execute_with_retry,
    fetch_all,
    fetch_one,
    execute_query,
    execute_many,
    
    # Wallet management
    add_wallet,
    get_wallet_for_user,
    get_user_wallets,
    update_wallet_state,
    check_ada_balance,
    update_ada_balance,
    update_token_balances,
    get_wallet_balance,
    update_utxo_state,
    get_stake_address,
    update_stake_address,
    remove_wallet_for_user,
    
    # Notification settings
    get_notification_settings,
    update_notification_setting,
    initialize_notification_settings,
    should_notify,
    
    # Database errors
    DatabaseError,
    ConnectionError,
    QueryError
)
from database_maintenance import DatabaseMaintenance
from cardano.address_validation import validate_cardano_address
from utils import (
    format_ada_amount,
    get_asset_info,
    parse_asset_id,
    format_token_amount,
    get_policy_info,
    get_token_info,
    validate_policy_id,
    validate_token_name
)

import random
import functools
from functools import wraps
from tenacity import retry, stop_after_attempt, wait_exponential

def get_request_id() -> str:
    """Generate a unique request ID for logging"""
    return str(uuid.uuid4())

def init_ssl_context() -> ssl.SSLContext:
    """Initialize SSL context with proper security settings"""
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True
    return ssl_context

# Suppress only the single InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def setup_logging():
    """Configure logging for the bot."""
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

# Create logger for this module
logger = logging.getLogger('walletbud.bot')

class RateLimiter:
    """Rate limiter with burst support and per-endpoint tracking"""
    def __init__(self, max_requests: int, burst_limit: int, cooldown_seconds: int):
        self.max_requests = max_requests
        self.burst_limit = burst_limit
        self.cooldown_seconds = cooldown_seconds
        self._endpoints = {}
        logger.info(f"Initialized RateLimiter with: max_requests={max_requests}, burst_limit={burst_limit}, cooldown_seconds={cooldown_seconds}")
    
    async def _get_endpoint(self, endpoint: str):
        """Get or create endpoint rate limit state"""
        if endpoint not in self._endpoints:
            self._endpoints[endpoint] = {
                'tokens': self.max_requests,
                'last_update': time.time(),
                'lock': asyncio.Lock()
            }
        return self._endpoints[endpoint]
    
    async def acquire(self, endpoint: str):
        """Acquire a rate limit token for the specified endpoint"""
        ep = await self._get_endpoint(endpoint)
        async with ep['lock']:
            current_time = time.time()
            time_passed = current_time - ep['last_update']
            
            # Replenish tokens based on time passed
            tokens_to_add = time_passed * (self.max_requests / self.cooldown_seconds)
            ep['tokens'] = min(ep['tokens'] + tokens_to_add, self.burst_limit)
            ep['last_update'] = current_time
            
            # Wait for token if needed
            while ep['tokens'] < 1:
                await asyncio.sleep(0.1)
                current_time = time.time()
                time_passed = current_time - ep['last_update']
                tokens_to_add = time_passed * (self.max_requests / self.cooldown_seconds)
                ep['tokens'] = min(ep['tokens'] + tokens_to_add, self.burst_limit)
                ep['last_update'] = current_time
            
            ep['tokens'] -= 1
    
    async def release(self, endpoint: str):
        """Release a rate limit token back to the specified endpoint"""
        ep = await self._get_endpoint(endpoint)
        async with ep['lock']:
            ep['tokens'] = min(ep['tokens'] + 1, self.burst_limit)

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
        intents.guilds = True
        intents.guild_messages = True
        intents.dm_messages = True
        
        # Initialize the bot with required parameters
        super().__init__(
            command_prefix='!',  # Required but not used since we use slash commands
            intents=intents,
            application_id=APPLICATION_ID,  # Use imported APPLICATION_ID
            *args,
            **kwargs
        )
        logger.info("Base bot initialized")
        
        # Initialize shutdown manager
        self.shutdown_manager = ShutdownManager()
        self.register_cleanup_handlers()
        
        # Get admin channel ID from environment
        self.admin_channel_id = ADMIN_CHANNEL_ID  # Use imported ADMIN_CHANNEL_ID
        self.admin_channel = None  # Will be set in setup_hook
        
        # Initialize components
        logger.info("Starting RateLimiter initialization...")
        self.rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND, BURST_LIMIT, RATE_LIMIT_COOLDOWN)
        logger.info("RateLimiter initialized")
        
        logger.info("Starting DatabaseMaintenance initialization...")
        self.db_maintenance = DatabaseMaintenance()
        logger.info("DatabaseMaintenance initialized")
        
        # Initialize database pool
        self.pool = None  # Will be initialized in setup_hook
        
        self.blockfrost_session = None
        self.session = None
        self.connector = None
        logger.info("Session variables initialized to None")
        
        # Initialize health metrics
        logger.info("Initializing health metrics...")
        self.health_metrics = {
            'start_time': None,
            'last_api_call': None,
            'last_db_query': None,
            'last_webhook': None,
            'blockfrost_init': None,
            'webhook_success': 0,
            'webhook_failure': 0,
            'last_health_check': None,
            'is_healthy': True,
            'discord_connection': None,
            'admin_channel': None,
            'discord_status': None,
            'connections': None,
            'errors': []
        }
        # Initialize health cache
        self._health_cache = TTLCache(maxsize=1, ttl=HEALTH_METRICS_TTL)
        logger.info("Health metrics initialized")
        
        # Initialize monitoring state
        self.monitoring_paused = False
        self._yummi_check_lock = None
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
        self._interaction_lock = None
        self.active_interactions = {}
        self.interaction_timeouts = {}
        self.webhook_retries = {}
        
        # Create tasks (but don't start them yet)
        self.check_yummi_balances = tasks.loop(hours=6)(self._check_yummi_balances)
        self.health_check_task = tasks.loop(minutes=5)(self.monitor_health)
        
        # Initialize SSL context with certificate verification enabled
        self.ssl_context = init_ssl_context()
        
        # Initialize aiohttp connector with default settings
        self.connector = None
        self.session = None
        
        # Add command locks
        self.command_locks = {}
        
        # Initialize webhook rate limiting
        self.webhook_rate_limits = {}
        
        # Initialize Discord rate limiting
        self._dm_rate_limits = {}
        self._global_dm_limit = {
            'tokens': 2,  # 2 messages per second globally
            'last_update': time.time(),
            'lock': None
        }
        logger.info("Rate limiting initialized")

    async def _ensure_interaction_lock(self):
        """Ensure interaction lock is created"""
        if self._interaction_lock is None:
            self._interaction_lock = asyncio.Lock()
        return self._interaction_lock

    async def _ensure_yummi_check_lock(self):
        """Ensure yummi check lock is created"""
        if self._yummi_check_lock is None:
            self._yummi_check_lock = asyncio.Lock()
        return self._yummi_check_lock

    async def _get_dm_rate_limit(self, user_id: int):
        """Get or create rate limit for user"""
        if user_id not in self._dm_rate_limits:
            self._dm_rate_limits[user_id] = {
                'tokens': 5,  # 5 messages per 5 seconds per user
                'last_update': time.time(),
                'lock': asyncio.Lock()
            }
        return self._dm_rate_limits[user_id]

    async def _ensure_global_dm_lock(self):
        """Ensure global DM lock is created"""
        if self._global_dm_limit['lock'] is None:
            self._global_dm_limit['lock'] = asyncio.Lock()
        return self._global_dm_limit['lock']

    def register_cleanup_handlers(self):
        """Register all cleanup handlers for graceful shutdown"""
        # Database cleanup
        self.shutdown_manager.register_handler(
            'database',
            self._cleanup_database
        )
        
        # Session cleanup
        self.shutdown_manager.register_handler(
            'session',
            self._cleanup_session
        )
        
        # Tasks cleanup
        self.shutdown_manager.register_handler(
            'tasks',
            self._cleanup_tasks
        )
        
        # Blockfrost cleanup
        self.shutdown_manager.register_handler(
            'blockfrost',
            self._cleanup_blockfrost
        )
        
        # Webhook server cleanup
        self.shutdown_manager.register_handler(
            'webhook',
            self._cleanup_webhook
        )
        
    async def _cleanup_database(self):
        """Cleanup database connections"""
        try:
            pool = await get_pool()
            await pool.close()
            logger.info("Database pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")
            
    async def _cleanup_session(self):
        """Cleanup aiohttp session"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            
            # Wait a bit to ensure all connections are properly closed
            await asyncio.sleep(0.25)
            
            if self.connector and not self.connector.closed:
                await self.connector.close()
                self.connector = None
                
            logger.info("HTTP session and connector closed successfully")
        except Exception as e:
            logger.error(f"Error closing HTTP session: {e}")
            
    async def _cleanup_tasks(self):
        """Cleanup background tasks"""
        try:
            # Cancel health check task
            if hasattr(self, 'health_check_task'):
                self.health_check_task.cancel()
                
            # Cancel YUMMI check task
            if hasattr(self, 'check_yummi_balances'):
                self.check_yummi_balances.cancel()
                
            logger.info("Background tasks cancelled successfully")
        except Exception as e:
            logger.error(f"Error cancelling background tasks: {e}")
            
    async def _cleanup_blockfrost(self):
        """Cleanup Blockfrost session"""
        try:
            if self.blockfrost_session and not self.blockfrost_session.closed:
                await self.blockfrost_session.close()
                logger.info("Blockfrost session cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up Blockfrost session: {e}")
        finally:
            self.blockfrost_session = None
            
    async def _cleanup_webhook(self):
        """Cleanup webhook server"""
        try:
            if hasattr(self, 'site') and self.site:
                await self.site.stop()
                logger.info("Webhook server stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping webhook server: {e}")
            
    async def close(self):
        """Clean up resources and perform graceful shutdown"""
        logger.info("Starting graceful shutdown...")
        
        try:
            # Cancel background tasks first
            await self._cancel_background_tasks()
            
            # Run shutdown handlers through shutdown manager
            await self.shutdown_manager.cleanup(timeout=30.0)
            
            # Finally call parent close
            await super().close()
            
            logger.info("Graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during graceful shutdown: {e}")
            # Still try to call parent close
            try:
                await super().close()
            except Exception as parent_error:
                logger.error(f"Error in parent close: {parent_error}")

    async def _cancel_background_tasks(self):
        """Cancel all background tasks"""
        try:
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling task: {e}")
            logger.info(f"Cancelled {len(tasks)} background tasks")
        except Exception as e:
            logger.error(f"Error in _cancel_background_tasks: {e}")

    async def setup_hook(self):
        """Set up the bot's background tasks and signal handlers"""
        try:
            logger.info("Starting setup_hook...")
            
            # Initialize database
            logger.info("Initializing database...")
            try:
                await init_db()
                logger.info("Database initialized successfully")
            except Exception as e:
                logger.error(f"Database initialization failed: {str(e)}")
                raise
            
            # Initialize Blockfrost client
            logger.info("Initializing Blockfrost client...")
            try:
                await self.init_blockfrost()
                logger.info("Blockfrost client initialized successfully")
            except Exception as e:
                logger.error(f"Blockfrost initialization failed: {str(e)}")
                raise
            
            # Load cogs
            logger.info("Loading cogs...")
            try:
                await self.load_extension("cogs.system_commands")
                logger.info("Loaded system_commands cog")
                await self.load_extension("cogs.wallet_commands")
                logger.info("Loaded wallet_commands cog")
                logger.info("All cogs loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load cogs: {str(e)}")
                raise
            
            # Sync command tree with Discord
            try:
                logger.info("Starting command tree sync...")
                await self.tree.sync()
                logger.info("Command tree synced successfully")
            except Exception as e:
                logger.error(f"Failed to sync command tree: {str(e)}")
                logger.error(f"Error type: {type(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                raise
            
            # Start background tasks
            logger.info("Starting background tasks...")
            try:
                self.check_yummi_balances.start()
                logger.info("Started yummi balance check task")
                self.health_check_task.start()
                logger.info("Started health check task")
                logger.info("All background tasks started successfully")
            except Exception as e:
                logger.error(f"Failed to start background tasks: {str(e)}")
                raise
            
            # Set start time
            self.health_metrics['start_time'] = datetime.utcnow()
            logger.info("Bot setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error in setup_hook: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")
            raise
            
    async def on_error(self, event_method: str, *args, **kwargs):
        """Called when an error occurs in an event"""
        error = sys.exc_info()
        if error:
            error_type, error_value, error_traceback = error
            
            # Log the error
            logger.error(
                f"Error in {event_method}: {error_type.__name__}: {str(error_value)}",
                exc_info=error
            )
            
            # Send error to admin channel
            if self.admin_channel:
                tb_str = "".join(traceback.format_tb(error_traceback))
                await self.admin_channel.send(
                    f" ERROR in {event_method}:\n"
                    f"```\n{error_type.__name__}: {str(error_value)}\n\n{tb_str}```"
                )
                
    async def process_interaction(self, interaction: discord.Interaction, ephemeral: bool = True):
        """Process interaction with proper error handling and retry logic"""
        interaction_id = str(interaction.id)
        
        try:
            # Lock to prevent concurrent processing of same interaction
            async with await self._ensure_interaction_lock():
                # Check if interaction is already being processed
                if interaction_id in self.active_interactions:
                    logger.warning(f"Interaction {interaction_id} is already being processed")
                    return
                
                # Mark interaction as active
                self.active_interactions[interaction_id] = {
                    'start_time': time.time(),
                    'status': 'processing'
                }
                
                try:
                    # Defer response immediately to prevent timeout
                    await interaction.response.defer(ephemeral=ephemeral)
                    
                    # Get command name
                    command_name = interaction.command.name if interaction.command else "unknown"
                    
                    # Check cooldown
                    if not await self._check_command_cooldown(interaction.user.id, command_name):
                        await interaction.followup.send(
                            "Please wait before using this command again.",
                            ephemeral=True
                        )
                        return
                        
                    # Get command handler
                    handler = self._get_command_handler(command_name)
                    if not handler:
                        logger.error(f"No handler found for command: {command_name}")
                        await interaction.followup.send(
                            "This command is not currently available.",
                            ephemeral=True
                        )
                        return
                        
                    # Execute command with timeout
                    try:
                        async with asyncio.timeout(30):  # 30 second timeout
                            await handler(interaction)
                            
                    except asyncio.TimeoutError:
                        logger.error(f"Command timed out: {command_name}")
                        await interaction.followup.send(
                            "The command took too long to process. Please try again.",
                            ephemeral=True
                        )
                        
                except discord.errors.InteractionResponded:
                    logger.debug(f"Interaction {interaction_id} already responded to")
                    
                except discord.errors.HTTPException as e:
                    logger.error(f"Discord HTTP error: {e}")
                    await self._handle_discord_error(interaction, e)
                    
                except Exception as e:
                    logger.error(f"Error processing command {command_name}: {e}")
                    await self._handle_command_error(interaction, e)
                    
        finally:
            # Clean up
            self.active_interactions.pop(interaction_id, None)
            
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
        """Send a direct message to a user with rate limiting"""
        try:
            # Check global rate limit
            async with await self._ensure_global_dm_lock():
                current_time = time.time()
                time_passed = current_time - self._global_dm_limit['last_update']
                
                if time_passed >= 1:  # 1 second cooldown
                    self._global_dm_limit['tokens'] = 2
                    self._global_dm_limit['last_update'] = current_time
                
                while self._global_dm_limit['tokens'] <= 0:
                    await asyncio.sleep(0.1)
                    current_time = time.time()
                    time_passed = current_time - self._global_dm_limit['last_update']
                    
                    if time_passed >= 1:
                        self._global_dm_limit['tokens'] = 2
                        self._global_dm_limit['last_update'] = current_time
                
                self._global_dm_limit['tokens'] -= 1
            
            # Check per-user rate limit
            async with (await self._get_dm_rate_limit(user_id))['lock']:
                current_time = time.time()
                time_passed = (await self._get_dm_rate_limit(user_id))['last_update'] - current_time
                
                if time_passed >= 5:  # 5 second cooldown
                    (await self._get_dm_rate_limit(user_id))['tokens'] = 5
                    (await self._get_dm_rate_limit(user_id))['last_update'] = current_time
                
                while (await self._get_dm_rate_limit(user_id))['tokens'] <= 0:
                    await asyncio.sleep(0.1)
                    current_time = time.time()
                    time_passed = (await self._get_dm_rate_limit(user_id))['last_update'] - current_time
                    
                    if time_passed >= 5:
                        (await self._get_dm_rate_limit(user_id))['tokens'] = 5
                        (await self._get_dm_rate_limit(user_id))['last_update'] = current_time
                
                (await self._get_dm_rate_limit(user_id))['tokens'] -= 1
            
            # Send the DM
            user = await self.fetch_user(user_id)
            if user:
                await user.send(content)
            else:
                logger.error(f"Failed to find user {user_id} for DM")
                
        except Exception as e:
            logger.error(f"Error sending DM: {str(e)}")

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of all system components"""
        try:
            # Try to get from cache first
            if hasattr(self, '_health_cache') and 'data' in self._health_cache:
                return self._health_cache['data']
                
            health_data = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat(),
                'components': {}
            }
            
            # Check Discord connection
            discord_health = {
                'healthy': self.is_ready(),
                'latency': round(self.latency * 1000, 2),  # ms
                'guilds': len(self.guilds),
                'shards': len(self.shards) if hasattr(self, 'shards') else 1
            }
            if not discord_health['healthy']:
                health_data['status'] = 'unhealthy'
                discord_health['error'] = 'Bot is not connected to Discord'
            health_data['components']['discord'] = discord_health
                
            # Check database connection
            try:
                if not self.pool:
                    await self.init_database()
                    
                if not self.pool:
                    raise Exception("Database pool not initialized")
                    
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                    pool_stats = await conn.fetchrow("""
                        SELECT 
                            count(*) as used_connections,
                            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_size,
                            (SELECT setting::int FROM pg_settings WHERE name = 'min_pool_size') as min_size
                        FROM pg_stat_activity
                    """)
                    db_health = {
                        'healthy': True,
                        'used_connections': pool_stats['used_connections'],
                        'max_size': pool_stats['max_size'],
                        'min_size': pool_stats['min_size']
                    }
            except Exception as e:
                health_data['status'] = 'unhealthy'
                db_health = {
                    'healthy': False,
                    'error': str(e)
                }
                # Try to reinitialize database connection
                try:
                    await self.init_database()
                except Exception as init_error:
                    logger.error(f"Failed to reinitialize database: {init_error}")
            health_data['components']['database'] = db_health
                
            # Check Blockfrost API
            try:
                bf_health_response = await self.blockfrost_request('/health')
                network_response = await self.blockfrost_request('/network')
                limits_response = await self.blockfrost_request('/metrics/endpoints')
                
                # Ensure responses are dictionaries
                bf_health_data = bf_health_response if isinstance(bf_health_response, dict) else {}
                network_data = network_response if isinstance(network_response, dict) else {}
                limits_data = limits_response if isinstance(limits_response, dict) else {}
                
                if isinstance(limits_response, list) and limits_response:
                    # If it's a list, take the first item
                    limits_data = limits_response[0]
                
                bf_health = {
                    'healthy': True,
                    'network': network_data.get('network', 'unknown'),
                    'sync_progress': network_data.get('sync_progress', 0),
                    'rate_limits': {
                        'remaining': limits_data.get('calls_remaining', 0),
                        'total': limits_data.get('calls_quota', 0)
                    }
                }
            except Exception as e:
                health_data['status'] = 'unhealthy'
                bf_health = {
                    'healthy': False,
                    'error': str(e)
                }
            health_data['components']['blockfrost'] = bf_health
            
            # Check system resources
            try:
                process = psutil.Process()
                system_health = {
                    'healthy': True,
                    'cpu_percent': process.cpu_percent(),
                    'memory_percent': process.memory_percent(),
                    'uptime': str(datetime.utcnow() - self.health_metrics['start_time']) if self.health_metrics.get('start_time') else 'unknown'
                }
                
                # Set degraded if high resource usage
                if system_health['cpu_percent'] > 80 or system_health['memory_percent'] > 80:
                    health_data['status'] = 'degraded'
                    system_health['warning'] = 'High resource usage'
                    
            except Exception as e:
                system_health = {
                    'healthy': False,
                    'error': str(e)
                }
            health_data['components']['system'] = system_health
            
            # Add recent errors if any
            recent_errors = [err for err in self.health_metrics.get('errors', [])[-5:]]
            if recent_errors:
                health_data['recent_errors'] = recent_errors
                
            # Cache the results
            if not hasattr(self, '_health_cache'):
                self._health_cache = TTLCache(maxsize=1, ttl=HEALTH_METRICS_TTL)
            self._health_cache['data'] = health_data
                
            return health_data
                
        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'components': {}
            }

    async def monitor_health(self):
        """Monitor bot health status"""
        try:
            # Perform health check
            health_data = await self.health_check()
            
            # Update metrics
            await self.update_health_metrics('last_health_check', datetime.utcnow())
            await self.update_health_metrics('is_healthy', health_data['status'] == 'healthy')
            
            # Log status and alert if needed
            if health_data['status'] == 'healthy':
                logger.debug("Health check passed")
            else:
                logger.warning(f"Health check reported status: {health_data['status']}")
                
                # Alert admin channel if configured
                if self.admin_channel:
                    # Create embed for health alert
                    embed = discord.Embed(
                        title=" System Health Alert",
                        description=f"Status: {health_data['status']}",
                        color=discord.Color.red()
                    )
                    
                    # Add component statuses
                    for component, data in health_data['components'].items():
                        if not data.get('healthy', False):
                            embed.add_field(
                                name=f"{component} Status",
                                value=data.get('error', 'No error details'),
                                inline=False
                            )
                    
                    # Add recent errors if any
                    if 'recent_errors' in health_data:
                        error_list = '\n'.join(health_data['recent_errors'][-3:])  # Show last 3 errors
                        embed.add_field(
                            name="Recent Errors",
                            value=error_list or "None",
                            inline=False
                        )
                    
                    await self.admin_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Health monitoring failed: {e}", exc_info=True)
            
    async def on_ready(self):
        """Called when the bot is ready and connected to Discord"""
        try:
            logger.info(f"Bot {self.user.name} is ready and connected to Discord!")
            logger.info(f"Bot ID: {self.user.id}")
            logger.info(f"Connected to {len(self.guilds)} guilds")
            
            # Log connection details
            await self.update_health_metrics('discord_connection', {
                'connected_at': datetime.utcnow().isoformat(),
                'guild_count': len(self.guilds),
                'latency': round(self.latency * 1000, 2)  # in ms
            })
            
            # Set up admin channel
            await self.setup_admin_channel()
            
            # Log successful startup to admin channel
            if self.admin_channel:
                embed = discord.Embed(
                    title="Bot Started Successfully",
                    description="WalletBud bot is now online and ready!",
                    color=discord.Color.green()
                )
                embed.add_field(
                    name="Connection Details",
                    value=f"Connected to {len(self.guilds)} guilds\nLatency: {round(self.latency * 1000, 2)}ms"
                )
                await self.admin_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in on_ready: {str(e)}")
            if hasattr(e, '__dict__'):
                logger.error(f"Error details: {e.__dict__}")

    async def setup_admin_channel(self):
        """Set up the admin channel for bot notifications"""
        try:
            if not self.admin_channel_id:
                logger.warning("No admin channel ID configured")
                return

            # Try to fetch the channel
            channel = self.get_channel(int(self.admin_channel_id))
            if not channel:
                logger.error(f"Could not find admin channel with ID {self.admin_channel_id}")
                return

            self.admin_channel = channel
            logger.info(f"Admin channel set up successfully: {channel.name}")
            
            # Update health metrics
            await self.update_health_metrics('admin_channel', {
                'id': self.admin_channel_id,
                'name': channel.name,
                'guild': channel.guild.name if channel.guild else None
            })
            
        except Exception as e:
            logger.error(f"Error setting up admin channel: {e}", exc_info=True)
            self.admin_channel = None

    async def on_connect(self):
        """Called when the bot connects to Discord"""
        try:
            logger.info("Bot connected to Discord")
            await self.update_health_metrics('discord_status', 'Connected')
            
            # Check all connections on connect
            logger.info("Checking all connections...")
            await self.check_connections()
            
            # Initialize components if needed
            if not self.pool:
                await self.init_database()
            if not self.blockfrost_session:
                await self.init_blockfrost()
                
        except Exception as e:
            logger.error(f"Error in on_connect: {e}", exc_info=True)

    async def on_disconnect(self):
        """Called when the bot disconnects from Discord"""
        try:
            logger.warning("Bot disconnected from Discord")
            await self.update_health_metrics('discord_status', 'Disconnected')
            
            # Update health metrics
            await self.update_health_metrics('is_healthy', False)
            
            # Log disconnection to admin channel if possible
            if self.admin_channel and hasattr(self.admin_channel, 'send'):
                try:
                    embed = discord.Embed(
                        title="Bot Disconnected",
                        description="Lost connection to Discord",
                        color=discord.Color.red()
                    )
                    await self.admin_channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Could not send disconnect notification: {e}")
                    
        except Exception as e:
            logger.error(f"Error in on_disconnect: {e}", exc_info=True)

    async def check_connections(self):
        """Check all connections and log their status"""
        try:
            # Check Discord connection
            discord_status = "Connected" if self.is_ready() else "Disconnected"
            logger.info(f"Discord Status: {discord_status}")
            await self.update_health_metrics('discord_status', discord_status)

            # Check database connection
            try:
                if not self.pool:
                    await self.init_database()
                    
                if not self.pool:
                    raise Exception("Database pool not initialized")
                    
                async with self.pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                    db_status = "Connected"
            except Exception as e:
                db_status = f"Error: {str(e)}"
                # Try to reinitialize database
                try:
                    await self.init_database()
                except Exception:
                    pass
            logger.info(f"Database Status: {db_status}")

            # Check Blockfrost connection
            try:
                health = await self.blockfrost_request('/health')
                bf_status = "Connected"
            except Exception as e:
                bf_status = f"Error: {str(e)}"
            logger.info(f"Blockfrost Status: {bf_status}")

            # Update overall connection status
            await self.update_health_metrics('connections', {
                'discord': discord_status,
                'database': db_status,
                'blockfrost': bf_status,
                'timestamp': datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"Error checking connections: {e}", exc_info=True)
            
    async def init_ssl_context() -> ssl.SSLContext:
        """Initialize SSL context with proper security settings"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True
        return ssl_context

    async def init_database(self):
        """Initialize database connection with proper error handling"""
        try:
            # Initialize the database pool
            self.pool = await get_pool()
            if not self.pool:
                raise RuntimeError("Failed to create database pool")
                
            # Test the connection
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
                
            logger.info("Database initialized successfully")
            if self.admin_channel:
                await self.admin_channel.send(" Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f" ERROR: Database initialization failed: {e}")
            raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client with proper error handling"""
        try:
            # Initialize connector and session
            self.connector = aiohttp.TCPConnector(ssl=self.ssl_context, limit=100)
            self.session = aiohttp.ClientSession(connector=self.connector)
            
            # Initialize Blockfrost session
            self.blockfrost_session = aiohttp.ClientSession(
                headers={'project_id': BLOCKFROST_PROJECT_ID},
                connector=self.connector
            )
            
            # Test connection using the full URL
            async with self.blockfrost_session.get(f"{BLOCKFROST_BASE_URL}/health") as response:
                if response.status != 200:
                    raise Exception(f"Blockfrost API health check failed: {response.status}")
                    
            logger.info("Blockfrost API initialized successfully")
            await self.update_health_metrics('blockfrost_init', datetime.now().isoformat())
            
            if self.admin_channel:
                await self.admin_channel.send(" Blockfrost API initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost API: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f" Error: Failed to initialize Blockfrost API: {e}")
            raise

    async def blockfrost_request(
                self, 
                endpoint: str,
                method: str = 'GET',
                **kwargs
            ):
        """Make a request to Blockfrost API with rate limiting and error handling
        
        Args:
            endpoint: API endpoint to call (e.g. '/health')
            method: HTTP method (GET, POST, etc.)
            **kwargs: Additional arguments to pass to aiohttp request
            
        Returns:
            API response as JSON
            
        Raises:
            Exception: If API request fails
        """
        try:
            # Ensure endpoint starts with /
            if not endpoint.startswith('/'):
                endpoint = '/' + endpoint
                
            # Construct full URL
            url = f"{BLOCKFROST_BASE_URL}{endpoint}"
            
            # Acquire rate limit token
            await self.rate_limiter.acquire('blockfrost')
            try:
                async with self.blockfrost_session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        raise Exception(f"Blockfrost API request failed: {response.status} - {error_text}")
            finally:
                # Release rate limit token
                self.rate_limiter.release('blockfrost')
                
        except Exception as e:
            logger.error(f"Error in Blockfrost request: {e}")
            raise
            
    async def on_resumed(self):
        """Called when the bot resumes a session"""
        logger.info("Session resumed")
        try:
            # Update presence
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(activity=activity, status=discord.Status.online)
            
            # Notify admin channel
            if self.admin_channel:
                await self.admin_channel.send(" Bot connection resumed!")
                
        except Exception as e:
            logger.error(f"Error in on_resumed: {str(e)}", exc_info=True)

    async def check_environment(self) -> bool:
        """Check if all required environment variables are set"""
        try:
            # Validate configuration through config module
            from config import validate_config
            validate_config()
            return True
        except Exception as e:
            logger.error(f"Environment validation failed: {e}")
            return False

    async def validate_and_init_dependencies(self):
        """Validate and initialize all critical dependencies"""
        try:
            # Initialize database
            await self.init_database()
            logger.info("Database initialized")
            
            # Initialize Blockfrost client
            await self.init_blockfrost()
            logger.info("Blockfrost client initialized")
            
            logger.info("All dependencies initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize dependencies: {e}")
            raise

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

    async def start_webhook(self):
        """Start the webhook server"""
        try:
            # Get port from environment
            port = int(os.getenv('PORT', 8080))
            
            # Initialize webhook components
            self.runner = web.AppRunner(app)  # Use global app
            await self.runner.setup()
            
            # Create site and start it
            self.site = web.TCPSite(self.runner, '0.0.0.0', port)
            await self.site.start()
            
            logger.info(f"Webhook server started on port {port}")
            
        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")
            raise

    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming Blockfrost webhooks"""
        request_id = get_request_id()
        logger.info(f"Received webhook request {request_id}")
        
        try:
            # Validate request method
            if request.method != 'POST':
                logger.warning(f"Invalid method {request.method} for webhook request {request_id}")
                return web.Response(status=405, text="Method not allowed")

            # Validate content type
            content_type = request.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                logger.warning(f"Invalid content type {content_type} for webhook request {request_id}")
                return web.Response(status=400, text="Invalid content type")

            # Get webhook data
            try:
                webhook_data = await request.json()
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in webhook request {request_id}: {str(e)}")
                return web.Response(status=400, text="Invalid JSON")

            # Validate webhook structure
            try:
                self._validate_webhook_structure(webhook_data)
            except ValueError as e:
                logger.error(f"Invalid webhook structure in request {request_id}: {str(e)}")
                return web.Response(status=400, text=str(e))

            # Verify Blockfrost signature
            signature = request.headers.get('Webhook-Signature')
            if not signature:
                logger.warning(f"Missing Blockfrost signature for webhook request {request_id}")
                return web.Response(status=401, text="Missing signature")

            try:
                payload = await request.read()
                expected_signature = hmac.new(
                    os.getenv('BLOCKFROST_WEBHOOK_SECRET').encode(),
                    payload,
                    hashlib.sha512
                ).hexdigest()
                
                if not hmac.compare_digest(signature, expected_signature):
                    logger.warning(f"Invalid signature for webhook request {request_id}")
                    return web.Response(status=401, text="Invalid signature")
            except Exception as e:
                logger.error(f"Error verifying signature for webhook request {request_id}: {str(e)}")
                return web.Response(status=500, text="Error verifying signature")

            # Process webhook based on type
            webhook_type = webhook_data.get('webhook_type')
            if webhook_type == 'transaction':
                await self._handle_transaction_webhook(webhook_data['payload'])
            elif webhook_type == 'delegation':
                await self._handle_delegation_webhook(webhook_data['payload'])
            else:
                logger.warning(f"Unknown webhook type: {webhook_type}")
                return web.Response(status=400, text="Unknown webhook type")

            logger.info(f"Successfully processed webhook request {request_id}")
            return web.Response(status=200, text="Webhook processed")

        except Exception as e:
            logger.error(f"Error processing webhook request {request_id}: {str(e)}")
            return web.Response(status=500, text="Internal server error")

    def _validate_webhook_structure(self, webhook_data: dict):
        """Validate webhook data structure"""
        required_fields = ['webhook_id', 'webhook_type', 'created_at', 'payload']
        for field in required_fields:
            if field not in webhook_data:
                raise ValueError(f"Missing required field: {field}")
                
        # Validate webhook ID
        webhook_id = webhook_data['webhook_id']
        tx_webhook_id = os.getenv('BLOCKFROST_TX_WEBHOOK_ID')
        del_webhook_id = os.getenv('BLOCKFROST_DEL_WEBHOOK_ID')
        
        if webhook_id not in [tx_webhook_id, del_webhook_id]:
            raise ValueError(f"Invalid webhook ID: {webhook_id}")
            
        # Validate webhook type
        webhook_type = webhook_data['webhook_type']
        if webhook_type not in ['transaction', 'delegation']:
            raise ValueError(f"Invalid webhook type: {webhook_type}")
            
        # Validate payload structure based on type
        payload = webhook_data['payload']
        if webhook_type == 'transaction':
            required_tx_fields = ['tx', 'block', 'confirmations']
            for field in required_tx_fields:
                if field not in payload:
                    raise ValueError(f"Missing required transaction field: {field}")
                    
        elif webhook_type == 'delegation':
            required_del_fields = ['stake_address', 'pool_id', 'amount']
            for field in required_del_fields:
                if field not in payload:
                    raise ValueError(f"Missing required delegation field: {field}")

    async def _handle_transaction_webhook(self, payload: dict):
        """Handle transaction webhook from Blockfrost"""
        try:
            # Extract transaction details
            tx_hash = payload['tx']['hash']
            block_height = payload['block']['height']
            confirmations = payload['confirmations']
            
            # Only process if we have enough confirmations
            min_confirmations = int(os.getenv('WEBHOOK_CONFIRMATIONS', 3))
            if confirmations < min_confirmations:
                logger.info(f"Transaction {tx_hash} has only {confirmations} confirmations, waiting for {min_confirmations}")
                return
                
            # Get transaction details
            tx_details = await self.blockfrost_request(f'/txs/{tx_hash}')
            
            # Process transaction outputs
            for output in tx_details['outputs']:
                # Get wallet address
                address = output['address']
                
                # Check if this is a monitored wallet
                async with self.pool.acquire() as conn:
                    wallet = await conn.fetchrow(
                        "SELECT user_id, notify_ada_transactions, notify_token_changes FROM wallets WHERE address = $1",
                        address
                    )
                    
                    if wallet:
                        # Send notification to user
                        user_id = wallet['user_id']
                        
                        # Create notification message
                        message = f" New transaction detected!\n"
                        message += f"Transaction: `{tx_hash}`\n"
                        message += f"Block Height: {block_height}\n"
                        message += f"Confirmations: {confirmations}\n"
                        
                        # Send DM to user
                        await self.send_dm(user_id, message)
                        
            logger.info(f"Successfully processed transaction webhook for tx {tx_hash}")
            
        except Exception as e:
            logger.error(f"Error processing transaction webhook: {e}")
            raise

    async def _handle_delegation_webhook(self, payload: dict):
        """Handle delegation webhook from Blockfrost"""
        try:
            # Extract delegation details
            stake_address = payload['stake_address']
            pool_id = payload['pool_id']
            amount = payload['amount']
            
            # Check if this is a monitored stake address
            async with self.pool.acquire() as conn:
                wallet = await conn.fetchrow(
                    "SELECT user_id, notify_delegation_status FROM wallets WHERE stake_address = $1",
                    stake_address
                )
                
                if wallet and wallet['notify_delegation_status']:
                    # Send notification to user
                    user_id = wallet['user_id']
                    
                    # Create notification message
                    message = f" Delegation Update!\n"
                    message += f"Stake Address: `{stake_address}`\n"
                    message += f"Pool: `{pool_id}`\n"
                    message += f"Amount: {amount} ADA\n"
                    
                    # Send DM to user
                    await self.send_dm(user_id, message)
                    
            logger.info(f"Successfully processed delegation webhook for stake address {stake_address}")
            
        except Exception as e:
            logger.error(f"Error processing delegation webhook: {e}")
            raise

    async def _check_yummi_balances(self):
        """Check YUMMI token balances for all wallets"""
        try:
            async with await self._ensure_yummi_check_lock():
                if self.processing_yummi:
                    logger.info("YUMMI balance check already in progress")
                    return
                    
                self.processing_yummi = True
                
            try:
                # Get all wallets
                pool = await get_pool()
                wallets = await pool.fetch("SELECT * FROM wallets")
                
                for wallet in wallets:
                    try:
                        # Skip if notifications disabled
                        if not await self.should_notify(wallet['user_id'], 'token_changes'):
                            continue
                            
                        # Get token balances
                        address = wallet['address']
                        token_balances = await self.blockfrost_request(f'/addresses/{address}/utxos')
                        
                        # Process token balances
                        for utxo in token_balances:
                            for amount in utxo['amount']:
                                if amount['unit'] != 'lovelace':  # Skip ADA
                                    # Check if it's a YUMMI token
                                    policy_id = amount['unit'][:56]
                                    if policy_id == os.getenv('YUMMI_POLICY_ID'):
                                        # Send notification
                                        message = f" YUMMI Token Update!\n"
                                        message += f"Address: `{address}`\n"
                                        message += f"Balance: {amount['quantity']} YUMMI\n"
                                        
                                        await self.send_dm(wallet['user_id'], message)
                                        
                    except Exception as e:
                        logger.error(f"Error checking YUMMI balance for wallet {wallet['address']}: {e}")
                        continue
                        
            finally:
                self.processing_yummi = False
                
        except Exception as e:
            logger.error(f"Error in YUMMI balance check: {e}")

async def main(app):
    """Main entry point for the bot when running locally"""
    try:
        # Initialize the bot
        await bot.start(os.getenv('DISCORD_TOKEN'))
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}")
        raise
    finally:
        if not bot.is_closed():
            await bot.close()

if __name__ == "__main__":
    # Create aiohttp app for local development
    app = web.Application()
    
    # Initialize bot instance
    bot = WalletBudBot()
    
    # Add webhook route and health check
    app.router.add_post('/webhook', bot.handle_webhook)
    app.router.add_get('/health', bot.health_check)
    
    # Add cleanup callback
    app.on_cleanup.append(bot.close)
    
    # Run the application
    try:
        web.run_app(
            app,
            port=int(os.getenv('PORT', 8080)),
            ssl_context=bot.ssl_context
        )
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
