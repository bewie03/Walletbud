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
    YUMMI_POLICY_ID, HEALTH_METRICS_TTL, WEBHOOK_CONFIG,
    validate_config
)

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
from shutdown_manager import ShutdownManager
from webhook_queue import WebhookQueue

import random
import functools
from functools import wraps
from tenacity import retry, stop_after_attempt, wait_exponential

# Third-party imports
import asyncpg
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from blockfrost import BlockFrostApi, ApiUrls
from blockfrost.api.cardano.network import network
from urllib3.exceptions import InsecureRequestWarning

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

    async def _check_yummi_balances(self):
        """Check Yummi token balances for all registered users"""
        try:
            logger.info("Starting Yummi balance check")
            async with self.pool.acquire() as conn:
                # Get all registered users
                users = await conn.fetch("SELECT user_id, stake_address FROM users WHERE stake_address IS NOT NULL")
                
                for user in users:
                    try:
                        # Get user's stake address assets
                        assets = await self.blockfrost_request(f'/accounts/{user["stake_address"]}/addresses/assets')
                        
                        # Find Yummi tokens
                        yummi_assets = [
                            asset for asset in assets 
                            if asset.get('unit', '').startswith(YUMMI_POLICY_ID)
                        ]
                        
                        if yummi_assets:
                            # Calculate total Yummi balance
                            total_yummi = sum(int(asset.get('quantity', 0)) for asset in yummi_assets)
                            
                            # Update user's Yummi balance
                            await conn.execute(
                                "UPDATE users SET yummi_balance = $1 WHERE user_id = $2",
                                total_yummi, user['user_id']
                            )
                            
                            logger.info(f"Updated Yummi balance for user {user['user_id']}: {total_yummi}")
                    except Exception as e:
                        logger.error(f"Error checking Yummi balance for user {user['user_id']}: {str(e)}")
                        continue
                        
            logger.info("Completed Yummi balance check")
            
        except Exception as e:
            logger.error(f"Error in Yummi balance check: {str(e)}")

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
        
        # Initialize webhook queue with secret
        logger.info("Initializing webhook queue...")
        self.webhook_queue = WebhookQueue(
            secret=WEBHOOK_SECRET,
            max_queue_size=WEBHOOK_CONFIG['MAX_QUEUE_SIZE'],
            batch_size=WEBHOOK_CONFIG['BATCH_SIZE'],
            max_retries=WEBHOOK_CONFIG['MAX_RETRIES'],
            max_event_age=WEBHOOK_CONFIG['MAX_EVENT_AGE'],
            cleanup_interval=WEBHOOK_CONFIG['CLEANUP_INTERVAL']
        )
        logger.info("Webhook queue initialized")
        
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
        try:
            # Register signal handlers
            for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGBREAK):
                try:
                    signal.signal(sig, self._handle_shutdown_signal)
                    logger.info(f"Registered signal handler for {sig.name}")
                except Exception as e:
                    logger.warning(f"Failed to register signal handler for {sig.name}: {e}")
                    
            # Register atexit handler
            atexit.register(self._handle_shutdown_atexit)
            logger.info("Registered atexit handler")
            
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
            
        except Exception as e:
            logger.error(f"Error registering cleanup handlers: {e}")
            
    async def _cleanup_database(self):
        """Cleanup database connections"""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None
                logger.info("Database pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")
            
    async def _cleanup_session(self):
        """Cleanup aiohttp session"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
                logger.info("Session closed successfully")
            if self.connector:
                await self.connector.close()
                self.connector = None
                logger.info("Connector closed successfully")
        except Exception as e:
            logger.error(f"Error closing session: {e}")
            
    async def _cleanup_blockfrost(self):
        """Cleanup Blockfrost session"""
        try:
            if self.blockfrost_session:
                await self.blockfrost_session.close()
                self.blockfrost_session = None
                logger.info("Blockfrost session cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up Blockfrost session: {e}")
        finally:
            self.blockfrost_session = None
            
    async def _cleanup_tasks(self):
        """Cleanup background tasks"""
        try:
            if hasattr(self, 'check_yummi_balances'):
                self.check_yummi_balances.cancel()
            if hasattr(self, 'health_check_task'):
                self.health_check_task.cancel()
            logger.info("Background tasks cancelled")
        except Exception as e:
            logger.error(f"Error cancelling tasks: {e}")
            
    async def _cleanup_webhook(self):
        """Cleanup webhook server"""
        try:
            # Additional webhook cleanup if needed
            pass
        except Exception as e:
            logger.error(f"Error cleaning up webhook: {e}")

    async def close(self):
        """Clean up resources and perform graceful shutdown"""
        logger.info("Starting graceful shutdown...")
        
        # Set shutdown flag
        self._shutdown = True
        
        try:
            # Cancel all background tasks
            await self._cancel_background_tasks()
            
            # Clean up components in order
            await self._cleanup_webhook()
            await self._cleanup_blockfrost()
            await self._cleanup_session()
            await self._cleanup_database()
            
            # Call parent cleanup
            await super().close()
            
            logger.info("Graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            raise
            
    async def _cancel_background_tasks(self):
        """Cancel all background tasks"""
        try:
            if self.check_yummi_balances.is_running():
                self.check_yummi_balances.cancel()
                
            if self.health_check_task.is_running():
                self.health_check_task.cancel()
                
            logger.info("Background tasks cancelled")
            
        except Exception as e:
            logger.error(f"Error cancelling background tasks: {e}")
            
    async def _cleanup_webhook(self):
        """Cleanup webhook server"""
        try:
            if hasattr(self, 'webhook_queue'):
                await self.webhook_queue.stop()
            logger.info("Webhook server stopped")
            
        except Exception as e:
            logger.error(f"Error cleaning up webhook server: {e}")
            
    async def _cleanup_blockfrost(self):
        """Cleanup Blockfrost session"""
        try:
            if self.blockfrost_session:
                await self.blockfrost_session.close()
                self.blockfrost_session = None
            logger.info("Blockfrost session closed")
            
        except Exception as e:
            logger.error(f"Error cleaning up Blockfrost session: {e}")
            
    async def _cleanup_session(self):
        """Cleanup aiohttp session"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
                
            if self.connector:
                await self.connector.close()
                self.connector = None
                
            logger.info("HTTP session closed")
            
        except Exception as e:
            logger.error(f"Error cleaning up HTTP session: {e}")
            
    async def _cleanup_database(self):
        """Cleanup database connections"""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None
            logger.info("Database connections closed")
            
        except Exception as e:
            logger.error(f"Error cleaning up database connections: {e}")
            
    async def setup_hook(self):
        """Set up the bot's background tasks and signal handlers"""
        try:
            # Initialize critical components
            await self.init_database()
            await self.init_blockfrost()
            
            # Start webhook queue
            logger.info("Starting webhook queue processor...")
            self.webhook_queue.start()
            
            # Start background tasks
            self.check_yummi_balances.start()
            self.health_check_task.start()
            
            # Get admin channel
            self.admin_channel = self.get_channel(self.admin_channel_id)
            if not self.admin_channel:
                logger.warning(f"Could not find admin channel with ID {self.admin_channel_id}")
            
            # Start webhook server
            await self.start_webhook()
            
            logger.info("Bot setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error in setup_hook: {str(e)}")
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
                'healthy': self.is_ready() and self.health_metrics.get('discord_connection', False),
                'latency': round(self.latency * 1000, 2),  # in ms
                'guild_count': len(self.guilds) if self.is_ready() else 0,
                'status': self.health_metrics.get('discord_status', 'unknown')
            }

            # Only mark as unhealthy if we're not in the startup phase
            startup_grace_period = 30  # seconds
            bot_uptime = (datetime.utcnow() - self.health_metrics.get('start_time', datetime.utcnow())).total_seconds() if self.health_metrics.get('start_time') else 0
            
            if not discord_health['healthy'] and bot_uptime > startup_grace_period:
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
                    'uptime': str(datetime.utcnow() - self.health_metrics.get('start_time', datetime.utcnow())) if self.health_metrics.get('start_time') else 'unknown'
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
            if not hasattr(self, '_ready_once'):
                self._ready_once = True
                logger.info(f"Bot {self.user.name} is ready and connected to Discord!")
                logger.info(f"Bot ID: {self.user.id}")
                logger.info(f"Connected to {len(self.guilds)} guilds")
                
                # Set up admin channel
                await self.setup_admin_channel()
                
                # Start background tasks if not already running
                if not self.check_yummi_balances.is_running():
                    self.check_yummi_balances.start()
                if not self.health_check_task.is_running():
                    self.health_check_task.start()
                
                # Update health metrics
                await self.update_health_metrics('start_time', datetime.now().isoformat())
                await self.update_health_metrics('discord_connection', True)
        except Exception as e:
            logger.error(f"Error in on_ready: {e}", exc_info=True)
            await self.update_health_metrics('discord_connection', False)
            
    async def on_connect(self):
        """Called when the bot connects to Discord"""
        try:
            logger.info("Bot connected to Discord")
            await self.update_health_metrics('discord_status', 'connected')
            await self.update_health_metrics('discord_connection', True)
            await self.check_connections()
        except Exception as e:
            logger.error(f"Error in on_connect: {e}", exc_info=True)

    async def on_disconnect(self):
        """Called when the bot disconnects from Discord"""
        try:
            logger.info("Bot disconnected from Discord")
            await self.update_health_metrics('discord_status', 'disconnected')
            await self.update_health_metrics('discord_connection', False)
            await self.check_connections()
        except Exception as e:
            logger.error(f"Error in on_disconnect: {e}", exc_info=True)

    async def check_connections(self):
        """Check all connections and log their status"""
        try:
            # Check Discord connection
            discord_status = "connected" if self.is_ready() and not self.is_closed() else "disconnected"
            logger.info(f"Discord Status: {discord_status}")
            await self.update_health_metrics('discord_status', discord_status)

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
                    db_status = "connected"
            except Exception as e:
                db_status = f"error: {str(e)}"
                # Try to reinitialize database
                try:
                    await self.init_database()
                except Exception:
                    pass
            logger.info(f"Database Status: {db_status}")

            # Check Blockfrost connection
            try:
                health = await self.blockfrost_request('/health')
                bf_status = "connected"
            except Exception as e:
                bf_status = f"error: {str(e)}"
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
            if self.pool is None:
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
            # Check if we already have a session
            if self.blockfrost_session:
                try:
                    # Test the session
                    await self.blockfrost_request('/health')
                    logger.info("Reusing existing Blockfrost session")
                    return
                except Exception:
                    # Session is invalid, close it
                    await self._cleanup_blockfrost()
            
            # Get project ID from environment
            project_id = os.getenv('BLOCKFROST_PROJECT_ID')
            if not project_id:
                raise Exception("BLOCKFROST_PROJECT_ID not set")
            
            # Create connector with proper SSL context
            connector = aiohttp.TCPConnector(
                limit=100,  # Connection pool size
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
                ssl=self.ssl_context
            )
            
            # Create session with proper timeout and headers
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.blockfrost_session = aiohttp.ClientSession(
                headers={
                    'project_id': project_id,
                    'Content-Type': 'application/json'
                },
                timeout=timeout,
                connector=connector,
                raise_for_status=True
            )
            
            # Store base URL
            self.blockfrost_base_url = os.getenv('BLOCKFROST_BASE_URL', 'https://cardano-mainnet.blockfrost.io/api/v0').rstrip('/')
            
            # Test the connection
            await self.blockfrost_request('/health')
            logger.info("Blockfrost session initialized successfully")
            await self.update_health_metrics('blockfrost_init', True)
            
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost: {e}", exc_info=True)
            await self.update_health_metrics('blockfrost_init', False)
            raise

    async def blockfrost_request(self, endpoint: str, method: str = 'GET', **kwargs):
        """
        Make a request to Blockfrost API with rate limiting and error handling
        
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
            # Initialize session if needed
            if not self.blockfrost_session:
                await self.init_blockfrost()
                if not self.blockfrost_session:
                    raise Exception("Failed to initialize Blockfrost session")
            
            # Add project ID to headers if not already present
            headers = kwargs.get('headers', {})
            if 'project_id' not in headers:
                headers['project_id'] = os.getenv('BLOCKFROST_PROJECT_ID')
            kwargs['headers'] = headers
            
            # Ensure URL is properly formed
            url = f"{self.blockfrost_base_url}/{endpoint.lstrip('/')}"
            
            # Make request with rate limiting
            try:
                await self.rate_limiter.acquire('blockfrost')
                async with self.blockfrost_session.request(method, url, **kwargs) as response:
                    if response.status == 403:
                        logger.error("Blockfrost API returned 403 - check project ID")
                        raise Exception("Invalid Blockfrost project ID")
                    response.raise_for_status()
                    return await response.json()
            finally:
                await self.rate_limiter.release('blockfrost')
                    
        except Exception as e:
            logger.error(f"Error in Blockfrost request: {endpoint} - {str(e)}")
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
        """Initialize webhook handling"""
        try:
            # Initialize webhook app
            app = web.Application()
            app.router.add_post('/webhook', self.handle_webhook)
            app.router.add_get('/health', self.health_check)
            
            # Add cleanup callback
            app.on_cleanup.append(self.close)
            
            # Get port from environment
            port = int(os.getenv('PORT', 8080))
            
            # Start webhook server
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.ssl_context)
            await site.start()
            
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
                logger.debug(f"Received webhook data: {json.dumps(webhook_data)}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in webhook request {request_id}: {str(e)}")
                return web.Response(status=400, text="Invalid JSON")

            # Verify Blockfrost signature
            signature = request.headers.get('Webhook-Signature')
            if not signature:
                logger.warning(f"Missing Blockfrost signature for webhook request {request_id}")
                return web.Response(status=401, text="Missing signature")

            try:
                # Get webhook secret
                webhook_secret = os.getenv('BLOCKFROST_WEBHOOK_SECRET')
                if not webhook_secret:
                    logger.error("BLOCKFROST_WEBHOOK_SECRET environment variable not set")
                    return web.Response(status=500, text="Server configuration error")

                # Read raw payload once
                payload = await request.read()
                if not payload:
                    logger.warning(f"Empty payload for webhook request {request_id}")
                    return web.Response(status=400, text="Empty payload")

                # Calculate expected signature
                expected_signature = hmac.new(
                    webhook_secret.encode(),
                    payload,
                    hashlib.sha512
                ).hexdigest()
                
                # Use constant-time comparison
                if not hmac.compare_digest(signature, expected_signature):
                    logger.warning(f"Invalid signature for webhook request {request_id}")
                    return web.Response(status=401, text="Invalid signature")

                # Parse webhook data
                try:
                    webhook_data = json.loads(payload)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in webhook request {request_id}: {str(e)}")
                    return web.Response(status=400, text="Invalid JSON")

                # Validate webhook data structure
                if not isinstance(webhook_data, dict):
                    logger.warning(f"Invalid webhook data type: {type(webhook_data)}")
                    return web.Response(status=400, text="Invalid webhook data format")

                # Get the payload, which might be nested or direct
                payload = webhook_data.get('payload', webhook_data)
                if not isinstance(payload, dict):
                    logger.warning(f"Invalid payload type: {type(payload)}")
                    return web.Response(status=400, text="Invalid payload format")

                # Validate required fields
                required_fields = ['type', 'data']
                missing_fields = [field for field in required_fields if field not in payload]
                if missing_fields:
                    logger.warning(f"Missing required fields in payload: {missing_fields}")
                    return web.Response(status=400, text=f"Missing required fields: {', '.join(missing_fields)}")

                # Validate webhook type
                webhook_type = payload['type']
                if not isinstance(webhook_type, str):
                    logger.warning(f"Invalid webhook type: {webhook_type}")
                    return web.Response(status=400, text="Invalid webhook type")

                # Validate webhook data
                webhook_data = payload['data']
                if not isinstance(webhook_data, dict):
                    logger.warning(f"Invalid webhook data: {webhook_data}")
                    return web.Response(status=400, text="Invalid webhook data")

                # Process webhook based on type
                try:
                    if webhook_type == 'transaction':
                        await self._handle_transaction_webhook(webhook_data)
                    elif webhook_type == 'delegation':
                        await self._handle_delegation_webhook(webhook_data)
                    else:
                        logger.warning(f"Unsupported webhook type: {webhook_type}")
                        return web.Response(status=400, text="Unsupported webhook type")

                    # Update metrics
                    await self.update_health_metrics('webhook_success')
                    return web.Response(status=200, text="Webhook processed successfully")

                except Exception as e:
                    logger.error(f"Error processing webhook: {str(e)}")
                    await self.update_health_metrics('webhook_failure')
                    return web.Response(status=500, text="Error processing webhook")

            except Exception as e:
                logger.error(f"Error handling webhook request {request_id}: {str(e)}")
                await self.update_health_metrics('webhook_failure')
                return web.Response(status=500, text="Internal server error")
        
        except Exception as e:
            logger.error(f"Error in handle_webhook: {str(e)}")
            await self.update_health_metrics('webhook_failure')
            return web.Response(status=500, text="Internal server error")

async def main(app):
    """Main entry point for the bot when running locally"""
    global bot
    try:
        # Initialize the bot
        bot = WalletBudBot()
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
