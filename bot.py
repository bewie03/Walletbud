import os
import re
import ssl
import sys
import time
import json
import uuid
import certifi
import asyncio
import logging
import discord
import aiohttp
import asyncpg
import requests
import traceback
import hmac
import hashlib
import psutil
from datetime import datetime, timedelta
from ipaddress import ip_network
from aiohttp import web
from discord.ext import commands, tasks
from typing import Dict, List, Any, Optional, Union, Callable
from collections import defaultdict
from urllib3.exceptions import InsecureRequestWarning
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
    update_pool_for_stake,
    get_database_url,
    DatabaseError
)

import discord
from discord import app_commands
from blockfrost import BlockFrostApi, ApiUrls
from blockfrost.api.cardano.network import network
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
    ERROR_MESSAGES,
    WEBHOOK_RETRY_ATTEMPTS,
    SSL_CERT_FILE
)

import uuid
import random
import functools
from functools import wraps
from cachetools import TTLCache
from tenacity import retry, stop_after_attempt, wait_exponential

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
        intents.guilds = True
        intents.guild_messages = True
        intents.dm_messages = True
        
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
        self.ssl_context = init_ssl_context()
        
        # Initialize aiohttp connector with default settings
        self.connector = aiohttp.TCPConnector(ssl=self.ssl_context, limit=100)
        
        # Add command locks
        self.command_locks = {}
        self.health_lock = asyncio.Lock()  # Specific lock for health command
        
        # Initialize webhook rate limiting
        self.webhook_rate_limits = {}
        
    async def setup_hook(self):
        """Set up the bot's background tasks"""
        try:
            # Initialize critical components
            await self.validate_and_init_dependencies()
            
            # Set up admin channel
            await self.setup_admin_channel()
            
            # Start background tasks with proper error handling
            try:
                self.check_yummi_balances.start()
            except RuntimeError as e:
                logger.error(f"Failed to start YUMMI check task: {e}")
                await self.send_admin_alert("⚠️ YUMMI check task failed to start", is_error=True)
                
            try:
                self._check_connection_task = self.loop.create_task(self._check_connection_loop())
            except Exception as e:
                logger.error(f"Failed to start connection check task: {e}")
                await self.send_admin_alert("⚠️ Connection check task failed to start", is_error=True)
                
            # Start webhook server
            try:
                await self.setup_webhook_handler()
            except Exception as e:
                logger.error(f"Failed to start webhook server: {e}")
                await self.send_admin_alert("⚠️ Webhook server failed to start", is_error=True)
                # Don't raise here as webhook is not critical for core functionality
            
            # Log successful setup
            logger.info("Bot setup completed successfully")
            await self.send_admin_alert("✅ Bot setup completed successfully", is_error=False)
            
        except Exception as e:
            logger.error(f"Failed to set up bot: {e}")
            await self.send_admin_alert(f"🚨 Bot setup failed: {e}", is_error=True)
            raise
            
    async def validate_and_init_dependencies(self):
        """Validate and initialize all critical dependencies"""
        try:
            # Validate environment first
            self.validate_environment()
            
            # Initialize database with retries
            retry_count = 0
            max_retries = 3
            while retry_count < max_retries:
                try:
                    await self.init_database()
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise RuntimeError(f"Database initialization failed after {max_retries} attempts: {e}")
                    logger.warning(f"Database initialization attempt {retry_count} failed: {e}")
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
            
            # Initialize Blockfrost client
            try:
                await self.init_blockfrost()
            except Exception as e:
                logger.error(f"Failed to initialize Blockfrost: {e}")
                await self.send_admin_alert("⚠️ Blockfrost initialization failed, some features may be limited", is_error=True)
                # Don't raise here as bot can function without Blockfrost
            
            # Initialize rate limiters
            self.init_rate_limiters()
            
            # Initialize SSL context
            self.ssl_context = self.init_ssl_context()
            
            # Initialize aiohttp session with proper error handling
            try:
                if self.session and not self.session.closed:
                    await self.session.close()
                    
                self.session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(
                        ssl=self.ssl_context,
                        limit=100,
                        ttl_dns_cache=300,
                        force_close=False
                    ),
                    timeout=aiohttp.ClientTimeout(
                        total=30,
                        connect=10,
                        sock_read=30
                    ),
                    raise_for_status=True
                )
            except Exception as e:
                logger.error(f"Failed to initialize aiohttp session: {e}")
                raise RuntimeError(f"HTTP session initialization failed: {e}")
            
        except Exception as e:
            logger.error(f"Failed to initialize dependencies: {e}")
            await self.send_admin_alert(f"🚨 Dependency initialization failed: {e}", is_error=True)
            raise RuntimeError(f"Dependency initialization failed: {e}")
            
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
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
            
    def init_ssl_context(self):
        """Initialize SSL context with proper security settings"""
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            return ssl_context
        except Exception as e:
            logger.error(f"Failed to initialize SSL context: {e}")
            raise RuntimeError(f"SSL context initialization failed: {e}")

    async def on_error(self, event_method: str, *args, **kwargs):
        """Called when an error occurs in an event"""
        try:
            error = sys.exc_info()
            
            # Format error details
            error_details = {
                'event': event_method,
                'error_type': error[0].__name__ if error[0] else 'Unknown',
                'error_message': str(error[1]) if error[1] else 'No message',
                'traceback': ''.join(traceback.format_tb(error[2])) if error[2] else 'No traceback'
            }
            
            # Log error
            logger.error(
                f"Error in {event_method}:\n"
                f"Type: {error_details['error_type']}\n"
                f"Message: {error_details['error_message']}\n"
                f"Traceback:\n{error_details['traceback']}"
            )
            
            # Send alert to admin channel
            alert_msg = (
                f"Error in event {event_method}:\n"
                f"```\n"
                f"Type: {error_details['error_type']}\n"
                f"Message: {error_details['error_message']}\n"
                f"```"
            )
            await self.send_admin_alert(alert_msg, is_error=True)
            
            # Update health metrics
            self.update_health_metrics('errors', error_details)
            
        except Exception as e:
            logger.error(f"Error in error handler: {e}")
            
    async def close(self):
        """Clean up resources when the bot is shutting down"""
        try:
            # Stop background tasks
            self.check_yummi_balances.cancel()
            if hasattr(self, '_check_connection_task'):
                self._check_connection_task.cancel()
                
            # Close webhook server
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()
                
            # Close aiohttp session
            if self.session:
                await self.session.close()
                
            # Close database pool
            if hasattr(self, '_pool') and self._pool:
                await self._pool.close()
                
            # Call parent close
            await super().close()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            
        finally:
            logger.info("Bot shutdown complete")
            
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
        """Process interaction with proper error handling and retry logic"""
        interaction_id = str(interaction.id)
        
        try:
            # Check if interaction is already being processed
            if interaction_id in self.active_interactions:
                logger.warning(f"Duplicate interaction received: {interaction_id}")
                return
                
            # Add to active interactions
            self.active_interactions[interaction_id] = True
            
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
            
    async def _check_command_cooldown(self, user_id: int, command: str) -> bool:
        """Check if user is on cooldown for command"""
        async with self.interaction_lock:
            now = time.time()
            key = f"{user_id}:{command}"
            
            # Clean up old cooldowns
            self._cleanup_cooldowns()
            
            # Check if on cooldown
            if key in self.interaction_cooldowns:
                last_use = self.interaction_cooldowns[key]
                if now - last_use < COMMAND_COOLDOWN:
                    return False
                    
            # Update cooldown
            self.interaction_cooldowns[key] = now
            return True
            
    def _cleanup_cooldowns(self):
        """Clean up expired cooldowns"""
        now = time.time()
        expired = [
            k for k, v in self.interaction_cooldowns.items()
            if now - v >= COMMAND_COOLDOWN
        ]
        for k in expired:
            del self.interaction_cooldowns[k]
            
    def _get_command_handler(self, command_name: str):
        """Get the appropriate command handler function"""
        handlers = {
            'balance': self._handle_balance_command,
            'stake': self._handle_stake_command,
            'rewards': self._handle_rewards_command,
            'transactions': self._handle_transactions_command,
            'settings': self._handle_settings_command,
            'help': self._handle_help_command,
            'health': self.health
        }
        return handlers.get(command_name)
        
    async def _handle_discord_error(self, interaction: discord.Interaction, error: Exception):
        """Handle Discord-specific errors"""
        try:
            if isinstance(error, discord.errors.Forbidden):
                await interaction.followup.send(
                    "I don't have permission to do that.",
                    ephemeral=True
                )
            elif isinstance(error, discord.errors.NotFound):
                await interaction.followup.send(
                    "The requested resource was not found.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "An error occurred while processing your command.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error handling Discord error: {e}")
            
    async def _handle_command_error(self, interaction: discord.Interaction, error: Exception):
        """Handle general command errors"""
        try:
            # Log error details
            logger.error(f"Command error: {error}", exc_info=True)
            
            # Send user-friendly error message
            await interaction.followup.send(
                "An error occurred while processing your command. Please try again later.",
                ephemeral=True
            )
            
            # Send detailed error to admin channel
            await self.send_admin_alert(
                f"🚨 Command error in {interaction.command.name if interaction.command else 'unknown'}: {error}",
                is_error=True
            )
            
        except Exception as e:
            logger.error(f"Error handling command error: {e}")

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

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of all system components"""
        health = {
            'status': 'healthy',
            'components': {},
            'metrics': {},
            'errors': []
        }
        
        try:
            # Check Discord connection
            health['components']['discord'] = {
                'status': 'connected' if self.is_ready() else 'disconnected',
                'latency': round(self.latency * 1000, 2),  # ms
                'guilds': len(self.guilds)
            }
            
            # Check Blockfrost
            try:
                if self.blockfrost:
                    await self.blockfrost.health()
                    health['components']['blockfrost'] = {
                        'status': 'healthy',
                        'last_call': self.health_metrics.get('last_api_call')
                    }
                else:
                    health['components']['blockfrost'] = {
                        'status': 'not_initialized'
                    }
            except Exception as e:
                health['components']['blockfrost'] = {
                    'status': 'error',
                    'error': str(e)
                }
                health['errors'].append(f"Blockfrost error: {str(e)}")
                
            # Check database
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute('SELECT 1')
                    health['components']['database'] = {
                        'status': 'connected',
                        'pool_size': self.pool.get_size(),
                        'free_size': self.pool.get_free_size()
                    }
            except Exception as e:
                health['components']['database'] = {
                    'status': 'error',
                    'error': str(e)
                }
                health['errors'].append(f"Database error: {str(e)}")
                
            # Check webhook server
            health['components']['webhook'] = {
                'status': 'running' if self.site else 'stopped',
                'queue_size': self._webhook_queue.qsize(),
                'success_count': self.health_metrics.get('webhook_success', 0),
                'failure_count': self.health_metrics.get('webhook_failure', 0)
            }
            
            # System metrics
            health['metrics'] = {
                'uptime': str(datetime.utcnow() - self.health_metrics['start_time']),
                'memory_usage': psutil.Process().memory_info().rss / 1024 / 1024,  # MB
                'cpu_percent': psutil.Process().cpu_percent(),
                'thread_count': psutil.Process().num_threads()
            }
            
            # Overall status
            if health['errors']:
                health['status'] = 'degraded' if len(health['errors']) < 2 else 'unhealthy'
                
        except Exception as e:
            health['status'] = 'error'
            health['errors'].append(f"Health check error: {str(e)}")
            
        return health
        
    @app_commands.command(name="health")
    @app_commands.checks.has_permissions(administrator=True)
    async def health(self, interaction: discord.Interaction):
        """Check bot health status"""
        try:
            # Run health check
            health = await self.health_check()
            
            # Create embed
            embed = discord.Embed(
                title="🏥 Bot Health Status",
                color=discord.Color.green() if health['status'] == 'healthy'
                else discord.Color.orange() if health['status'] == 'degraded'
                else discord.Color.red()
            )
            
            # Components
            components = []
            for name, info in health['components'].items():
                status_emoji = "✅" if info['status'] in ['healthy', 'connected', 'running'] else "⚠️" if info['status'] == 'degraded' else "❌"
                components.append(f"{status_emoji} **{name.title()}**: {info['status']}")
            embed.add_field(
                name="Components",
                value="\n".join(components),
                inline=False
            )
            
            # Metrics
            metrics = []
            for name, value in health['metrics'].items():
                if isinstance(value, float):
                    value = f"{value:.2f}"
                metrics.append(f"**{name.replace('_', ' ').title()}**: {value}")
            embed.add_field(
                name="Metrics",
                value="\n".join(metrics),
                inline=False
            )
            
            # Errors
            if health['errors']:
                embed.add_field(
                    name="⚠️ Errors",
                    value="\n".join(f"- {error}" for error in health['errors'][-5:]),  # Show last 5 errors
                    inline=False
                )
                
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error in health command: {e}")
            await interaction.response.send_message(
                "❌ Error running health check. Check logs for details.",
                ephemeral=True
            )

    async def on_ready(self):
        """Called when the bot is ready and connected to Discord"""
        try:
            logger.info(f"Logged in as {self.user.name} ({self.user.id})")
            
            # Set up admin channel
            if self.admin_channel_id:
                await self.setup_admin_channel()
                logger.info("Admin channel setup complete")
            
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
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check database
                try:
                    async with self._pool.acquire() as conn:
                        await conn.execute('SELECT 1')
                except Exception as e:
                    logger.error(f"Database connection check failed: {e}")
                    await self.send_admin_alert("⚠️ Database connection lost, attempting to reconnect...")
                    await self.init_database()
                
                # Check Blockfrost
                if not self.blockfrost:
                    logger.warning("Blockfrost client not initialized, attempting to initialize...")
                    await self.init_blockfrost()
                else:
                    try:
                        await self.blockfrost_request(self.blockfrost.health)
                    except Exception as e:
                        logger.error(f"Blockfrost connection check failed: {e}")
                        await self.send_admin_alert("⚠️ Blockfrost connection lost, attempting to reconnect...")
                        await self.init_blockfrost()
                
                # Check webhook server
                if not self.site or self.site.closed:
                    logger.warning("Webhook server not running, attempting to restart...")
                    await self.setup_webhook_handler()
                
            except Exception as e:
                logger.error(f"Error in connection check loop: {e}")
                await self.send_admin_alert(f"⚠️ Connection check failed: {e}")
                await asyncio.sleep(60)  # Wait longer on error
                
    async def init_blockfrost(self):
        """Initialize Blockfrost API client with proper error handling and retries"""
        MAX_RETRIES = 3
        RETRY_DELAY = 5
        
        for attempt in range(MAX_RETRIES):
            try:
                project_id = os.getenv('BLOCKFROST_PROJECT_ID')
                if not project_id:
                    raise ValueError("BLOCKFROST_PROJECT_ID not set")
                    
                # Create client with proper SSL context
                self.blockfrost = BlockFrostApi(
                    project_id=project_id,
                    base_url=os.getenv('BLOCKFROST_BASE_URL', ApiUrls.mainnet.value),
                    session_args={'ssl': self.ssl_context}
                )
                
                # Test connection
                await self.blockfrost_request(self.blockfrost.health)
                logger.info("Blockfrost client initialized successfully")
                return
                
            except Exception as e:
                logger.error(f"Blockfrost initialization attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    await self.send_admin_alert(f"🚨 Blockfrost initialization failed: {e}")
                    raise
                    
    async def blockfrost_request(self, method: Callable, *args, **kwargs):
        """Execute a rate-limited request to Blockfrost API with retries"""
        if not self.blockfrost:
            raise ValueError("Blockfrost client not initialized")

        if self.fallback_mode:
            logger.warning(f"Blockfrost request attempted in fallback mode: {method.__name__}")
            raise RuntimeError("Blockfrost is in fallback mode")

        # Get rate limiters
        global_limiter = self.rate_limiters['blockfrost']['global']
        endpoint_limiter = self.rate_limiters['blockfrost']['endpoints'][method.__name__]

        for attempt in range(WEBHOOK_RETRY_ATTEMPTS):
            try:
                # Apply both global and endpoint-specific rate limiting
                async with global_limiter:
                    async with endpoint_limiter:
                        response = await method(*args, **kwargs)
                        
                        # Update metrics
                        self.update_health_metrics('last_api_call', datetime.now())
                        return response

            except Exception as e:
                error_msg = str(e).lower()
                wait_time = min(2 ** attempt * 1.5, 30)  # Max 30 second delay

                if "rate limit" in error_msg:
                    logger.warning(f"Rate limit hit: {error_msg}")
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.info(f"Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise ValueError("Rate limit exceeded. Please try again later.")

                elif "not found" in error_msg:
                    logger.info(f"Resource not found: {error_msg}")
                    return None

                elif any(msg in error_msg for msg in ["timeout", "connection", "network"]):
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.warning(f"Connection error, retrying in {wait_time}s: {error_msg}")
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"Connection failed after {WEBHOOK_RETRY_ATTEMPTS} retries")
                    raise

                else:
                    logger.error(f"Blockfrost API error: {error_msg}")
                    if hasattr(e, '__dict__'):
                        logger.error(f"Error details: {e.__dict__}")
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
            await self.change_presence(status=discord.Status.online, activity=activity)
            
            # Notify admin channel
            if self.admin_channel:
                await self.admin_channel.send("🟢 Bot connection resumed!")
                
        except Exception as e:
            logger.error(f"Error in on_resumed: {str(e)}", exc_info=True)

    async def check_webhook_rate_limit(self, ip: str) -> bool:
        """Check if webhook request exceeds rate limit"""
        # Use a more granular rate limit per IP
        rate_key = f"webhook:{ip}"
        now = datetime.utcnow()
        
        async with self.rate_limiter.acquire("webhook"):
            # Get current state
            state = self.webhook_rate_limits.get(rate_key, {
                'count': 0,
                'reset_at': now
            })
            
            # Reset if window has passed
            if now > state['reset_at']:
                state = {
                    'count': 0,
                    'reset_at': now + timedelta(minutes=1)
                }
                
            # Check limit
            if state['count'] >= WEBHOOK_RATE_LIMIT:
                return False
                
            # Update state
            state['count'] += 1
            self.webhook_rate_limits[rate_key] = state
            
            # Clean up old entries every 100 requests
            if random.random() < 0.01:  # 1% chance
                self._cleanup_rate_limits()
                
            return True
            
    def _cleanup_rate_limits(self):
        """Clean up expired rate limit entries"""
        now = datetime.utcnow()
        expired = [
            k for k, v in self.webhook_rate_limits.items()
            if now > v['reset_at']
        ]
        for k in expired:
            del self.webhook_rate_limits[k]
            
    async def handle_webhook(self, request: web.Request):
        """Handle incoming webhooks with comprehensive validation and rate limiting"""
        try:
            # Get client IP
            peername = request.transport.get_extra_info('peername')
            if peername is None:
                logger.error("Could not get client IP")
                return web.Response(status=400, text="Invalid request")
            
            client_ip = peername[0]
            
            # Validate IP
            if not self.is_valid_ip(client_ip):
                logger.warning(f"Invalid IP attempt: {client_ip}")
                return web.Response(status=403, text="Forbidden")
            
            # Check rate limit
            if not await self.check_webhook_rate_limit(client_ip):
                logger.warning(f"Rate limit exceeded for IP: {client_ip}")
                return web.Response(status=429, text="Too Many Requests")
            
            # Validate auth token
            auth_header = request.headers.get('Authorization')
            if not auth_header or auth_header != os.getenv('WEBHOOK_AUTH_TOKEN'):
                logger.warning(f"Invalid auth token from IP: {client_ip}")
                return web.Response(status=401, text="Unauthorized")
            
            try:
                # Parse JSON body with size limit
                if request.content_length and request.content_length > 1024 * 1024:  # 1MB limit
                    logger.warning(f"Request too large from IP: {client_ip}")
                    return web.Response(status=413, text="Payload Too Large")
                    
                body = await request.json()
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from IP: {client_ip}")
                return web.Response(status=400, text="Invalid JSON")
                
            except Exception as e:
                logger.error(f"Error reading request body: {e}")
                return web.Response(status=400, text="Invalid request body")
            
            # Validate webhook identifier
            if not body.get('identifier') == os.getenv('WEBHOOK_IDENTIFIER'):
                logger.warning(f"Invalid webhook identifier from IP: {client_ip}")
                return web.Response(status=400, text="Invalid webhook identifier")
            
            # Queue webhook for processing
            try:
                # Add request metadata
                body['_metadata'] = {
                    'ip': client_ip,
                    'timestamp': datetime.utcnow().isoformat(),
                    'request_id': str(uuid.uuid4())
                }
                
                # Put in queue with timeout
                try:
                    await asyncio.wait_for(
                        self._webhook_queue.put(body),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logger.error("Webhook queue is full")
                    return web.Response(status=503, text="Service Unavailable")
                
                self.health_metrics['webhook_success'] += 1
                return web.Response(status=202, text="Accepted")
                
            except Exception as e:
                logger.error(f"Error queueing webhook: {e}")
                self.health_metrics['webhook_failure'] += 1
                return web.Response(status=500, text="Internal Server Error")
            
        except Exception as e:
            logger.error(f"Webhook handler error: {e}")
            return web.Response(status=500, text="Internal Server Error")

    async def _process_webhook_queue(self):
        """Process webhooks from queue with error handling"""
        while True:
            try:
                # Get webhook from queue
                webhook_data = await self._webhook_queue.get()
                
                # Extract metadata
                metadata = webhook_data.pop('_metadata', {})
                request_id = metadata.get('request_id', 'unknown')
                
                try:
                    # Process webhook with timeout
                    await asyncio.wait_for(
                        self._process_single_webhook(webhook_data, request_id),
                        timeout=30.0
                    )
                    
                except asyncio.TimeoutError:
                    logger.error(f"Webhook processing timed out: {request_id}")
                    await self.send_admin_alert(f"⚠️ Webhook processing timed out: {request_id}", is_error=True)
                    
                except Exception as e:
                    logger.error(f"Error processing webhook {request_id}: {e}")
                    await self.send_admin_alert(f"⚠️ Webhook processing error: {e}", is_error=True)
                    
                finally:
                    # Always mark task as done
                    self._webhook_queue.task_done()
                
            except Exception as e:
                logger.error(f"Webhook queue processor error: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on error
                
    async def _process_single_webhook(self, webhook_data: dict, request_id: str):
        """Process a single webhook with proper error handling"""
        try:
            # Validate webhook data structure
            if not self._validate_webhook_structure(webhook_data):
                logger.error(f"Invalid webhook structure: {request_id}")
                return
            
            # Process based on webhook type
            webhook_type = webhook_data.get('type')
            processor = self._get_webhook_processor(webhook_type)
            
            if processor:
                await processor(webhook_data)
            else:
                logger.warning(f"Unknown webhook type: {webhook_type}")
            
        except Exception as e:
            logger.error(f"Error processing webhook {request_id}: {e}")
            raise  # Re-raise for the main processor to handle
            
    def _validate_webhook_structure(self, webhook_data: dict) -> bool:
        """Validate webhook data structure"""
        required_fields = ['type', 'data']
        return all(field in webhook_data for field in required_fields)
        
    def _get_webhook_processor(self, webhook_type: str):
        """Get the appropriate webhook processor function"""
        processors = {
            'transaction': self._process_transaction_webhook,
            'stake': self._process_stake_webhook,
            'delegation': self._process_delegation_webhook,
            'reward': self._process_reward_webhook
        }
        return processors.get(webhook_type)

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

    async def blockfrost_request(self, method: Callable, endpoint: str = None, *args, **kwargs) -> Any:
        """Execute a rate-limited request to Blockfrost API with retries and error handling"""
        if not self.blockfrost:
            raise ValueError("Blockfrost client not initialized")

        if self.fallback_mode:
            logger.warning(f"Blockfrost request attempted in fallback mode: {method.__name__}")
            raise RuntimeError("Blockfrost is in fallback mode")

        # Get rate limiters
        global_limiter = self.rate_limiters['blockfrost']['global']
        endpoint_limiter = self.rate_limiters['blockfrost']['endpoints'][endpoint or method.__name__]

        for attempt in range(WEBHOOK_RETRY_ATTEMPTS):
            try:
                # Apply both global and endpoint-specific rate limiting
                async with global_limiter:
                    async with endpoint_limiter:
                        response = await method(*args, **kwargs)
                        
                        # Update metrics
                        self.update_health_metrics('last_api_call', datetime.now())
                        return response

            except Exception as e:
                error_msg = str(e).lower()
                wait_time = min(2 ** attempt * 1.5, 30)  # Max 30 second delay

                if "rate limit" in error_msg:
                    logger.warning(f"Rate limit hit: {error_msg}")
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.info(f"Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise ValueError("Rate limit exceeded. Please try again later.")

                elif "not found" in error_msg:
                    logger.info(f"Resource not found: {error_msg}")
                    return None

                elif any(msg in error_msg for msg in ["timeout", "connection", "network"]):
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.warning(f"Connection error, retrying in {wait_time}s: {error_msg}")
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"Connection failed after {WEBHOOK_RETRY_ATTEMPTS} retries")
                    raise

                else:
                    logger.error(f"Blockfrost API error: {error_msg}")
                    if hasattr(e, '__dict__'):
                        logger.error(f"Error details: {e.__dict__}")
                    raise

    async def init_blockfrost(self):
        """Initialize Blockfrost API client with proper error handling and retries"""
        if not os.getenv('BLOCKFROST_PROJECT_ID'):
            raise ValueError("BLOCKFROST_PROJECT_ID environment variable is not set")
            
        try:
            base_url = os.getenv('BLOCKFROST_BASE_URL', ApiUrls.mainnet.value)
            self.blockfrost = BlockFrostApi(
                project_id=os.getenv('BLOCKFROST_PROJECT_ID'),
                base_url=base_url
            )
            # Test connection
            await self.blockfrost.health()
            logger.info("Blockfrost client initialized successfully")
            self.health_metrics['blockfrost_init'] = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost client: {e}")
            self.blockfrost = None
            raise

    async def blockfrost_request(self, method: Callable, *args, **kwargs):
        """Execute a rate-limited request to Blockfrost API with retries"""
        if not self.blockfrost:
            raise RuntimeError("Blockfrost client not initialized")
            
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                async with self.rate_limiter.acquire("blockfrost"):
                    response = await method(*args, **kwargs)
                    self.health_metrics['last_api_call'] = datetime.utcnow()
                    return response
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Blockfrost request failed after {max_retries} attempts: {e}")
                    raise
                    
                logger.warning(f"Blockfrost request failed (attempt {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                
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

    async def _check_yummi_balances(self):
        """Check YUMMI token balances with proper concurrency control"""
        # Use a task name based lock to prevent duplicate runs
        lock_key = 'yummi_balance_check'
        
        if self._locks.get(lock_key) and not self._locks[lock_key].locked():
            self._locks[lock_key] = asyncio.Lock()
            
        async with self._locks[lock_key]:
            try:
                if self.fallback_mode:
                    logger.warning("Skipping YUMMI balance check - in fallback mode")
                    return
                    
                # Get registered wallets
                async with self.pool.acquire() as conn:
                    wallets = await conn.fetch(
                        "SELECT address FROM wallets WHERE notify_balance_changes = true"
                    )
                
                # Process in batches to avoid rate limits
                batch_size = 50
                for i in range(0, len(wallets), batch_size):
                    batch = wallets[i:i + batch_size]
                    
                    # Process batch concurrently with rate limiting
                    tasks = []
                    for wallet in batch:
                        task = asyncio.create_task(
                            self.check_single_wallet_balance(wallet['address'])
                        )
                        tasks.append(task)
                    
                    # Wait for batch to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Handle any errors
                    for addr, result in zip([w['address'] for w in batch], results):
                        if isinstance(result, Exception):
                            logger.error(f"Error checking balance for {addr}: {result}")
                            
                    # Rate limit between batches
                    if i + batch_size < len(wallets):
                        await asyncio.sleep(1)
                        
            except asyncio.CancelledError:
                logger.info("YUMMI balance check cancelled")
                raise
            except Exception as e:
                logger.error(f"Error in YUMMI balance check: {e}", exc_info=True)
                raise
            finally:
                # Update health metrics
                self.update_health_metrics('last_balance_check', datetime.now())

    async def check_single_wallet_balance(self, address: str) -> None:
        """Check balance for a single wallet with retries"""
        try:
            # Get current balance
            current_balance = await self.blockfrost_request(
                self.blockfrost.address_assets,
                address
            )
            
            # Get previous balance from database
            async with self.pool.acquire() as conn:
                prev_balance = await conn.fetchval(
                    "SELECT yummi_balance FROM wallets WHERE address = $1",
                    address
                )
            
            # Find YUMMI token in current balance
            yummi_balance = 0
            for asset in current_balance:
                if asset.unit == YUMMI_POLICY_ID:
                    yummi_balance = int(asset.quantity)
                    break
            
            # Compare and notify if changed
            if prev_balance != yummi_balance:
                await self.notify_balance_change(
                    address, 
                    prev_balance or 0, 
                    yummi_balance
                )
                
                # Update database
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE wallets 
                        SET yummi_balance = $1, 
                            last_balance_check = NOW() 
                        WHERE address = $2
                        """,
                        yummi_balance, 
                        address
                    )
                    
        except Exception as e:
            logger.error(f"Error checking balance for {address}: {e}")
            raise

    async def check_environment(self) -> bool:
        """Check if all required environment variables are set"""
        required_vars = {
            "DISCORD_TOKEN": "Discord bot token",
            "APPLICATION_ID": "Discord application ID",
            "ADMIN_CHANNEL_ID": "Admin channel ID",
            "BLOCKFROST_PROJECT_ID": "Blockfrost project ID",
            "BLOCKFROST_BASE_URL": "Blockfrost base URL",
            "DATABASE_URL": "Database connection URL"
        }
        
        missing_vars = []
        invalid_vars = []
        
        for var, description in required_vars.items():
            value = os.getenv(var)
            if not value:
                missing_vars.append(f"{var} ({description})")
                continue
                
            # Validate specific variables
            try:
                if var == "APPLICATION_ID":
                    int(value)  # Should be a valid integer
                elif var == "ADMIN_CHANNEL_ID":
                    int(value)  # Should be a valid integer
                elif var == "BLOCKFROST_PROJECT_ID":
                    if not value.startswith(("mainnet", "preprod", "preview")):
                        invalid_vars.append(f"{var} (Should start with mainnet, preprod, or preview)")
                elif var == "DATABASE_URL":
                    if not value.startswith(("postgresql://", "postgres://")):
                        invalid_vars.append(f"{var} (Invalid PostgreSQL URL format)")
            except ValueError:
                invalid_vars.append(f"{var} (Invalid format)")
        
        if missing_vars or invalid_vars:
            if missing_vars:
                logger.error("Missing required environment variables:\n" + 
                           "\n".join(f"• {var}" for var in missing_vars))
            if invalid_vars:
                logger.error("Invalid environment variables:\n" + 
                           "\n".join(f"• {var}" for var in invalid_vars))
            return False
            
        return True

    async def validate_and_init_dependencies(self):
        """Validate and initialize all critical dependencies"""
        try:
            # 1. Validate environment variables
            if not await self.validate_environment():
                raise RuntimeError("Environment validation failed")

            # 2. Initialize SSL context
            if not await self.init_ssl_context():
                raise RuntimeError("SSL context initialization failed")

            # 3. Initialize database
            if not await self.init_database():
                raise RuntimeError("Database initialization failed")

            # 4. Initialize Blockfrost
            if not await self.init_blockfrost():
                logger.error("⚠️ Blockfrost initialization failed. Starting in fallback mode.")
                self.fallback_mode = True
            else:
                self.fallback_mode = False

            # 5. Initialize rate limiters
            self.init_rate_limiters()

            # 6. Initialize webhook handler if secret is present
            if os.getenv('WEBHOOK_SECRET'):
                await self.setup_webhook_handler()
            else:
                logger.warning("WEBHOOK_SECRET not set. Webhook functionality disabled.")

            return True

        except Exception as e:
            logger.error(f"Failed to initialize dependencies: {e}", exc_info=True)
            return False

    async def validate_environment(self) -> bool:
        """Validate all required environment variables"""
        required_vars = {
            'DISCORD_TOKEN': {
                'description': 'Discord bot token',
                'validator': lambda x: len(x) > 50  # Basic token length check
            },
            'DATABASE_URL': {
                'description': 'PostgreSQL connection URL',
                'validator': lambda x: x.startswith(('postgresql://', 'postgres://'))
            },
            'BLOCKFROST_PROJECT_ID': {
                'description': 'Blockfrost project ID',
                'validator': lambda x: x.startswith(('mainnet', 'preprod', 'preview'))
            },
            'BLOCKFROST_BASE_URL': {
                'description': 'Blockfrost API base URL',
                'validator': lambda x: x.startswith('https://')
            },
            'ADMIN_CHANNEL_ID': {
                'description': 'Admin channel ID for notifications',
                'validator': lambda x: x.isdigit()
            }
        }

        missing_vars = []
        invalid_vars = []

        for var_name, config in required_vars.items():
            value = os.getenv(var_name)
            if not value:
                missing_vars.append(f"{var_name} ({config['description']})")
                continue
                
            try:
                if not config['validator'](value):
                    invalid_vars.append(f"{var_name} (invalid format)")
            except Exception:
                invalid_vars.append(f"{var_name} (validation error)")

        if missing_vars or invalid_vars:
            if missing_vars:
                logger.error("Missing required environment variables:\n" + 
                           "\n".join(f"• {var}" for var in missing_vars))
            if invalid_vars:
                logger.error("Invalid environment variables:\n" + 
                           "\n".join(f"• {var}" for var in invalid_vars))
            return False

        return True

    async def init_ssl_context(self) -> bool:
        """Initialize SSL context with proper security settings"""
        try:
            # Create SSL context with strong security settings
            ssl_context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=certifi.where()
            )
            
            # Set minimum TLS version to 1.2
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            
            # Enable certificate verification
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.check_hostname = True
            
            # Disable weak ciphers
            ssl_context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20')
            
            return ssl_context
        except Exception as e:
            logger.error(f"Failed to initialize SSL context: {e}")
            raise RuntimeError(f"SSL context initialization failed: {e}")

    def init_rate_limiters(self):
        """Initialize rate limiters for different services"""
        self.rate_limiters = {
            'blockfrost': {
                'global': RateLimiter(
                    rate=RATE_LIMITS['blockfrost']['calls_per_second'],
                    burst=RATE_LIMITS['blockfrost']['burst']
                ),
                'endpoints': defaultdict(lambda: RateLimiter(
                    rate=RATE_LIMITS['blockfrost']['calls_per_second'] / 2,
                    burst=RATE_LIMITS['blockfrost']['burst'] / 2
                ))
            },
            'discord': RateLimiter(
                rate=RATE_LIMITS['discord']['global_rate_limit'],
                burst=RATE_LIMITS['discord']['command_rate_limit']
            )
        }

    async def blockfrost_request(self, method: Callable, *args, **kwargs) -> Any:
        """Execute a rate-limited request to Blockfrost API with retries and error handling"""
        if not self.blockfrost:
            raise ValueError("Blockfrost client not initialized")

        if self.fallback_mode:
            logger.warning(f"Blockfrost request attempted in fallback mode: {method.__name__}")
            raise RuntimeError("Blockfrost is in fallback mode")

        # Get rate limiters
        global_limiter = self.rate_limiters['blockfrost']['global']
        endpoint_limiter = self.rate_limiters['blockfrost']['endpoints'][method.__name__]

        for attempt in range(WEBHOOK_RETRY_ATTEMPTS):
            try:
                # Apply both global and endpoint-specific rate limiting
                async with global_limiter:
                    async with endpoint_limiter:
                        response = await method(*args, **kwargs)
                        
                        # Update metrics
                        self.update_health_metrics('last_api_call', datetime.now())
                        return response

            except Exception as e:
                error_msg = str(e).lower()
                wait_time = min(2 ** attempt * 1.5, 30)  # Max 30 second delay

                if "rate limit" in error_msg:
                    logger.warning(f"Rate limit hit: {error_msg}")
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.info(f"Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise ValueError("Rate limit exceeded. Please try again later.")

                elif "not found" in error_msg:
                    logger.info(f"Resource not found: {error_msg}")
                    return None

                elif any(msg in error_msg for msg in ["timeout", "connection", "network"]):
                    if attempt < WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.warning(f"Connection error, retrying in {wait_time}s: {error_msg}")
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"Connection failed after {WEBHOOK_RETRY_ATTEMPTS} retries")
                    raise

                else:
                    logger.error(f"Blockfrost API error: {error_msg}")
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
        
        # Get port from environment for Heroku
        port = int(os.getenv('PORT', 8080))
        
        # Start webhook server on 0.0.0.0 for Heroku
        async def start_webhook():
            """Start the webhook server with proper error handling and Heroku compatibility"""
            try:
                # Initialize aiohttp app
                app = web.Application()
                app.router.add_post('/webhook', bot.handle_webhook)
                
                # Set up runner with proper cleanup
                runner = web.AppRunner(app, access_log=None)  # Disable access logging for performance
                await runner.setup()
                
                # Get port from environment (Heroku sets PORT)
                port = int(os.getenv("PORT", 8080))
                
                # Bind to 0.0.0.0 for Heroku
                site = web.TCPSite(runner, "0.0.0.0", port)
                
                try:
                    await site.start()
                    logger.info(f"Webhook server started on port {port}")
                except OSError as e:
                    logger.error(f"Failed to bind to port {port}: {e}")
                    # Try alternative port if 8080 is taken
                    if port == 8080:
                        alt_port = 8081
                        site = web.TCPSite(runner, "0.0.0.0", alt_port)
                        await site.start()
                        logger.info(f"Webhook server started on alternative port {alt_port}")
                
                return runner, site
                
            except Exception as e:
                logger.error(f"Failed to start webhook server: {e}")
                raise
        
        # Run the bot and webhook server
        loop.create_task(start_webhook())
        bot.run(os.getenv('DISCORD_TOKEN'))
        
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}", exc_info=True)
        sys.exit(1)
