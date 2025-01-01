import os
import re
import ssl
import sys
import json
import time
import signal
import psutil
import asyncio
import logging
import certifi
import discord
import aiohttp
from aiohttp import web
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

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
            application_id=APPLICATION_ID,  # Use imported APPLICATION_ID
            *args,
            **kwargs
        )
        
        # Initialize shutdown manager
        self.shutdown_manager = ShutdownManager()
        self.register_cleanup_handlers()
        
        # Get admin channel ID from environment
        self.admin_channel_id = ADMIN_CHANNEL_ID  # Use imported ADMIN_CHANNEL_ID
        self.admin_channel = None  # Will be set in setup_hook
        
        # Initialize components
        self.rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND, BURST_LIMIT, RATE_LIMIT_COOLDOWN)
        self.db_maintenance = DatabaseMaintenance()
        self.blockfrost_session = None
        self.session = None
        self.connector = None
        
        # Initialize webhook components
        self.app = web.Application()
        self.runner = None
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
        self.connector = None
        self.session = None
        
        # Add command locks
        self.command_locks = {}
        self.health_lock = asyncio.Lock()  # Specific lock for health command
        
        # Initialize webhook rate limiting
        self.webhook_rate_limits = {}
        
        # Initialize health check task
        self.health_check_task = tasks.loop(minutes=5)(self.monitor_health)
        # Task will be started in setup_hook
        self.health_lock = asyncio.Lock()
        
    def register_cleanup_handlers(self):
        """Register all cleanup handlers for graceful shutdown"""
        # Database cleanup
        self.shutdown_manager.register_handler(
            'database',
            self._cleanup_database
        )
        
        # Webhook cleanup
        self.shutdown_manager.register_handler(
            'webhook',
            self._cleanup_webhook
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
        
    async def _cleanup_database(self):
        """Cleanup database connections"""
        try:
            pool = await get_pool()
            await pool.close()
            logger.info("Database pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")
            
    async def _cleanup_webhook(self):
        """Cleanup webhook server and queue"""
        try:
            # Cancel webhook processor
            if self._webhook_processor and not self._webhook_processor.done():
                self._webhook_processor.cancel()
                try:
                    await self._webhook_processor
                except asyncio.CancelledError:
                    pass
                    
            # Close webhook site
            if self.site:
                await self.site.stop()
                
            # Close webhook runner
            if self.runner:
                await self.runner.cleanup()
                
            logger.info("Webhook components cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up webhook components: {e}")
            
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
            # Initialize connector and session
            self.connector = aiohttp.TCPConnector(ssl=self.ssl_context, limit=100)
            self.session = aiohttp.ClientSession(connector=self.connector)
            
            # Initialize dependencies
            await self.validate_and_init_dependencies()
            
            # Set up admin channel
            await self.setup_admin_channel()
            
            # Load cogs
            self.load_extension("cogs.system_commands")
            self.load_extension("cogs.wallet_commands")
            logger.info("Loaded all cogs")
            
            # Start background tasks
            self.health_check_task.start()
            self.check_yummi_balances.start()
            
            # Set up signal handlers
            loop = asyncio.get_running_loop()
            self.shutdown_manager.setup_signal_handlers(loop)
            
            logger.info("Bot setup completed successfully")
        except Exception as e:
            logger.error(f"Error in setup_hook: {e}")
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
        try:
            current_time = datetime.utcnow()
            health_data = {
                'status': 'healthy',
                'components': {},
                'last_check': current_time.isoformat(),
                'uptime': (current_time - self.health_metrics['start_time']).total_seconds() if self.health_metrics['start_time'] else 0
            }

            # Check Discord connection
            health_data['components']['discord'] = {
                'status': 'connected' if self.is_ready() else 'disconnected',
                'latency': round(self.latency * 1000, 2)  # Convert to ms
            }

            # Check database connection
            try:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.fetchval('SELECT 1')
                health_data['components']['database'] = {
                    'status': 'connected',
                    'last_query': self.health_metrics['last_db_query'].isoformat() if self.health_metrics['last_db_query'] else None
                }
            except Exception as e:
                logger.error(f"Database health check failed: {e}")
                health_data['components']['database'] = {
                    'status': 'error',
                    'error': str(e)
                }
                health_data['status'] = 'degraded'

            # Check Blockfrost API
            try:
                if self.blockfrost_session:
                    async with self.blockfrost_session.get('/health') as response:
                        if response.status == 200:
                            health = await response.json()
                            if health.get('is_healthy'):
                                health_data['components']['blockfrost'] = {
                                    'status': 'connected',
                                    'last_call': self.health_metrics['last_api_call'].isoformat() if self.health_metrics['last_api_call'] else None
                                }
                            else:
                                raise Exception(f"Blockfrost API is not healthy: {health}")
                        else:
                            raise Exception(f"Health check failed with status {response.status}")
                else:
                    health_data['components']['blockfrost'] = {'status': 'not_initialized'}
                    health_data['status'] = 'degraded'
            except Exception as e:
                logger.error(f"Blockfrost health check failed: {e}")
                health_data['components']['blockfrost'] = {
                    'status': 'error',
                    'error': str(e)
                }
                health_data['status'] = 'degraded'

            # Check webhook server
            health_data['components']['webhook'] = {
                'status': 'running' if self.site else 'stopped',
                'queue_size': self._webhook_queue.qsize() if self._webhook_queue else 0,
                'last_webhook': self.health_metrics['last_webhook'].isoformat() if self.health_metrics['last_webhook'] else None
            }

            # Check system resources
            process = psutil.Process()
            health_data['components']['system'] = {
                'cpu_percent': process.cpu_percent(),
                'memory_percent': process.memory_percent(),
                'threads': process.num_threads()
            }

            # Log health status
            if health_data['status'] != 'healthy':
                logger.warning(f"Health check returned degraded status: {json.dumps(health_data, indent=2)}")
            else:
                logger.info("Health check passed successfully")

            return health_data

        except Exception as e:
            logger.error(f"Health check failed: {e}\n{traceback.format_exc()}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

    @app_commands.command(name="health")
    @app_commands.checks.has_permissions(administrator=True)
    async def health(self, interaction: discord.Interaction):
        """Check bot health status"""
        try:
            # Run health check
            health = await self.health_check()
            
            # Create embed
            embed = discord.Embed(
                title=" Bot Health Status",
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
                " Error running health check. Check logs for details.",
                ephemeral=True
            )

    async def on_ready(self):
        """Called when the bot is ready and connected to Discord"""
        try:
            # Set bot status
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="Cardano wallets | /help"
            )
            await self.change_presence(activity=activity, status=discord.Status.online)
            logger.info("Bot status updated")
            
            # Update health metrics
            await self.update_health_metrics('start_time', datetime.now().isoformat())
            
            # Log successful initialization
            logger.info(f"Logged in as {self.user} ({self.user.id})")
            logger.info("Bot initialization complete")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {e}")
            
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
            discord_status = "connected" if self.is_ready() else "disconnected"
            logger.debug(f"Discord status: {discord_status}")
            
            # Check database connection
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                db_status = "connected"
            except Exception as e:
                logger.error(f"Database connection error: {e}")
                db_status = "disconnected"
            
            # Check Blockfrost connection
            try:
                async with self.blockfrost_session.get('/health') as response:
                    if response.status == 200:
                        health = await response.json()
                        if health.get('is_healthy'):
                            blockfrost_status = "connected"
                        else:
                            raise Exception(f"Blockfrost API is not healthy: {health}")
                    else:
                        raise Exception(f"Health check failed with status {response.status}")
            except Exception as e:
                logger.error(f"Blockfrost connection error: {e}")
                blockfrost_status = "disconnected"
            
            # Check webhook server
            webhook_status = "running" if self.site and not self.site._closed else "stopped"
            
            # Update health metrics
            self.update_health_metrics('connections', {
                'discord': discord_status,
                'database': db_status,
                'blockfrost': blockfrost_status,
                'webhook': webhook_status
            })
            
            # Log overall status
            logger.info(f"Connection status - Discord: {discord_status}, DB: {db_status}, "
                       f"Blockfrost: {blockfrost_status}, Webhook: {webhook_status}")
            
            return {
                'discord': discord_status,
                'database': db_status,
                'blockfrost': blockfrost_status,
                'webhook': webhook_status
            }
            
        except Exception as e:
            logger.error(f"Error checking connections: {e}")
            raise

    async def setup_admin_channel(self):
        """Set up admin channel for bot notifications"""
        try:
            if not self.admin_channel_id:
                logger.warning("Admin channel ID not set")
                return
                
            # Get the admin channel
            self.admin_channel = await self.fetch_channel(self.admin_channel_id)
            if not self.admin_channel:
                logger.error("Could not find admin channel")
                return
                
            logger.info("Admin channel setup successful")
            await self.admin_channel.send(" Bot is starting up...")
            
        except Exception as e:
            logger.error(f"Failed to set up admin channel: {e}")
            # Don't raise here as bot can function without admin channel

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
        if not BLOCKFROST_PROJECT_ID:
            logger.error("BLOCKFROST_PROJECT_ID environment variable is not set")
            if self.admin_channel:
                await self.admin_channel.send("Error: BLOCKFROST_PROJECT_ID not set")
            raise ValueError("BLOCKFROST_PROJECT_ID environment variable is not set")
            
        try:
            # Close existing session if any
            if self.blockfrost_session and not self.blockfrost_session.closed:
                await self.blockfrost_session.close()
            
            # Create new session
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
            self.blockfrost_session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    'project_id': BLOCKFROST_PROJECT_ID
                }
            )
            
            base_url = 'https://cardano-mainnet.blockfrost.io/api/v0'
            
            # Test connection by checking root endpoint first
            async with self.blockfrost_session.get(f'{base_url}/') as response:
                if response.status == 200:
                    root = await response.json()
                    logger.info("Blockfrost root endpoint accessible")
                    
                    # Now check health endpoint
                    async with self.blockfrost_session.get(f'{base_url}/health') as health_response:
                        if health_response.status == 200:
                            health = await health_response.json()
                            if health.get('is_healthy'):
                                logger.info("Blockfrost client initialized successfully")
                                if self.admin_channel:
                                    await self.admin_channel.send("Blockfrost API connection established")
                                
                                # Update health metrics
                                self.update_health_metrics('blockfrost_init', datetime.now().isoformat())
                            else:
                                raise Exception(f"Blockfrost API is not healthy: {health}")
                        else:
                            raise Exception(f"Health check failed with status {health_response.status}")
                else:
                    raise Exception(f"Root endpoint check failed with status {response.status}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Blockfrost client: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f"Error: Failed to initialize Blockfrost client: {e}")
            if self.blockfrost_session and not self.blockfrost_session.closed:
                await self.blockfrost_session.close()
                self.blockfrost_session = None
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
        if not hasattr(self, 'blockfrost_session') or not self.blockfrost_session:
            raise Exception("Blockfrost session not initialized")
            
        try:
            async with self.blockfrost_session.request(method, endpoint, **kwargs) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    # Update health metrics
                    self.update_health_metrics('last_api_call', datetime.now().isoformat())
                    
                    return result
                else:
                    error = await response.json()
                    raise Exception(f"Blockfrost API request failed: {error}")
            
        except Exception as e:
            logger.error(f"Blockfrost API request failed: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f"Error: Blockfrost API request failed: {e}")
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
                await self.admin_channel.send(" Bot connection resumed!")
                
        except Exception as e:
            logger.error(f"Error in on_resumed: {str(e)}", exc_info=True)

    async def start_webhook(self):
        """Start the webhook server"""
        try:
            # Get port from environment
            port = int(os.getenv('PORT', 8080))
            
            # Create site and start it
            self.site = web.TCPSite(self.runner, '0.0.0.0', port)
            await self.site.start()
            
            logger.info(f"Webhook server started on port {port}")
            
        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")
            raise

    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhooks with comprehensive validation and rate limiting"""
        request_id = get_request_id()
        self.log_sanitized('info', f"Received webhook request {request_id}")
        
        try:
            # Get client IP and check rate limit
            client_ip = request.headers.get('X-Forwarded-For', request.remote).split(',')[0].strip()
            if not self.check_webhook_rate_limit(client_ip):
                self.log_sanitized('warning', f"Rate limit exceeded for IP {client_ip}")
                return web.Response(status=429, text="Rate limit exceeded")

            # Validate request method
            if request.method != 'POST':
                self.log_sanitized('warning', f"Invalid method {request.method} for webhook request {request_id}")
                return web.Response(status=405, text="Method not allowed")

            # Validate content type
            content_type = request.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                self.log_sanitized('warning', f"Invalid content type {content_type} for webhook request {request_id}")
                return web.Response(status=400, text="Invalid content type")

            # Validate request size
            content_length = request.content_length
            if content_length is None or content_length > MAX_WEBHOOK_SIZE:
                self.log_sanitized('warning', f"Invalid content length {content_length} for webhook request {request_id}")
                return web.Response(status=413, text="Payload too large")

            # Validate Blockfrost signature
            signature = request.headers.get('Webhook-Signature')
            if not signature:
                self.log_sanitized('warning', f"Missing Blockfrost signature for webhook request {request_id}")
                return web.Response(status=401, text="Missing signature")

            # Get webhook data
            try:
                webhook_data = await request.json()
            except json.JSONDecodeError as e:
                self.log_sanitized('error', f"Invalid JSON in webhook request {request_id}: {str(e)}")
                return web.Response(status=400, text="Invalid JSON")

            # Validate webhook structure
            try:
                self._validate_webhook_structure(webhook_data)
            except ValueError as e:
                self.log_sanitized('error', f"Invalid webhook structure in request {request_id}: {str(e)}")
                return web.Response(status=400, text=str(e))

            # Verify Blockfrost signature
            try:
                payload = await request.read()
                expected_signature = hmac.new(
                    WEBHOOK_SECRET.encode(),
                    payload,
                    hashlib.sha512
                ).hexdigest()
                
                if not hmac.compare_digest(signature, expected_signature):
                    self.log_sanitized('warning', f"Invalid signature for webhook request {request_id}")
                    return web.Response(status=401, text="Invalid signature")
            except Exception as e:
                self.log_sanitized('error', f"Error verifying signature for webhook request {request_id}: {str(e)}")
                return web.Response(status=500, text="Error verifying signature")

            # Process webhook
            await self._webhook_queue.put({
                'data': webhook_data,
                'request_id': request_id,
                'timestamp': time.time()
            })

            self.log_sanitized('info', f"Successfully queued webhook request {request_id}")
            return web.Response(status=202, text="Webhook accepted")

        except Exception as e:
            self.log_sanitized('error', f"Error processing webhook request {request_id}: {str(e)}")
            return web.Response(status=500, text="Internal server error")
            
    async def _process_webhook_queue(self):
        """Process webhooks from queue with error handling"""
        while True:
            try:
                # Get webhook data from queue with timeout
                webhook_data, request_id = await self._webhook_queue.get()
                
                # Process the webhook
                try:
                    await self._process_single_webhook(webhook_data, request_id)
                    self.health_metrics['webhook_success'] += 1
                    if self.admin_channel:
                        await self.admin_channel.send(f" SUCCESS: Processed webhook {request_id}")
                    
                except Exception as e:
                    # Handle webhook processing failure
                    await self._handle_webhook_failure(webhook_data, request_id, str(e))
                    
                finally:
                    # Mark task as done
                    self._webhook_queue.task_done()
                    
            except asyncio.CancelledError:
                logger.info("Webhook processor task cancelled")
                break
                
            except Exception as e:
                logger.error(f"Error in webhook processor: {e}")
                if self.admin_channel:
                    await self.admin_channel.send(f" ERROR: Webhook processor error: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on persistent errors
                
    async def _handle_webhook_failure(self, webhook_data: dict, request_id: str, error: str):
        """Handle webhook processing failure with smart retry logic"""
        try:
            # Update health metrics
            self.health_metrics['webhook_failure'] += 1
            
            # Log the failure
            logger.error(f"Webhook processing failed for request {request_id}: {error}")
            
            # Get current retry count
            retry_count = self.webhook_retries.get(request_id, 0)
            
            if retry_count < MAX_RETRIES:
                # Increment retry count
                self.webhook_retries[request_id] = retry_count + 1
                
                # Calculate delay with exponential backoff
                delay = min(300, 2 ** retry_count)  # Cap at 5 minutes
                
                # Log retry attempt
                logger.info(f"Retrying webhook {request_id} in {delay} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
                
                # Send admin notification
                if self.admin_channel:
                    await self.admin_channel.send(
                        f" WARNING: Webhook processing failed for request {request_id}. "
                        f"Retrying in {delay} seconds (attempt {retry_count + 1}/{MAX_RETRIES})"
                    )
                
                # Schedule retry
                await asyncio.sleep(delay)
                await self._webhook_queue.put({
                    'data': webhook_data,
                    'request_id': request_id,
                    'timestamp': time.time()
                })

            else:
                # Log permanent failure
                logger.error(f"Webhook {request_id} failed permanently after {MAX_RETRIES} retries")
                
                # Send admin notification
                if self.admin_channel:
                    await self.admin_channel.send(
                        f" ERROR: Webhook {request_id} failed permanently after {MAX_RETRIES} retries.\n"
                        f"Error: {error}"
                    )
                
                # Clean up retry counter
                self.webhook_retries.pop(request_id, None)
                
        except Exception as e:
            logger.error(f"Error handling webhook failure: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f" ERROR: Failed to handle webhook failure: {e}")
                
    async def _process_single_webhook(self, webhook_data: dict, request_id: str):
        """Process a single webhook with proper error handling"""
        try:
            # Validate webhook structure
            if not self._validate_webhook_structure(webhook_data):
                logger.error(f"Invalid webhook structure for request {request_id}")
                if self.admin_channel:
                    await self.admin_channel.send(f" ERROR: Invalid webhook structure for request {request_id}")
                return
                
            # Get webhook type and handler
            webhook_type = webhook_data.get('type')
            handler = self._get_webhook_processor(webhook_type)
            
            if not handler:
                logger.error(f"No handler found for webhook type: {webhook_type}")
                if self.admin_channel:
                    await self.admin_channel.send(f" ERROR: No handler found for webhook type: {webhook_type}")
                return
                
            # Process webhook
            await handler(webhook_data)
            logger.info(f"Successfully processed webhook {request_id}")
            
        except Exception as e:
            logger.error(f"Error processing webhook {request_id}: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f" ERROR: Failed to process webhook {request_id}: {e}")
            raise
            
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
            
            # Initialize webhook components
            self.app.router.add_post('/webhook', self.handle_webhook)
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            logger.info("Webhook components initialized")
            
            # Start webhook processor
            self._webhook_processor = asyncio.create_task(self._process_webhook_queue())
            logger.info("Webhook processor started")
            
            logger.info("All dependencies initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize dependencies: {e}")
            raise

    async def start_webhook(self):
        """Start the webhook server"""
        try:
            # Get port from environment
            port = int(os.getenv('PORT', 8080))
            
            # Create site and start it
            self.site = web.TCPSite(self.runner, '0.0.0.0', port)
            await self.site.start()
            
            logger.info(f"Webhook server started on port {port}")
            
        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")
            raise

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
            async with self.blockfrost_session.get(f'/addresses/{address}/assets') as response:
                if response.status == 200:
                    current_balance = await response.json()
                else:
                    raise Exception(f"Failed to get balance for {address}: {response.status}")
            
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

    async def monitor_health(self):
        """Background task to monitor system health"""
        try:
            # Run health check
            await self.health_check()
            
            # Log results
            logger.info("Health check completed successfully")
            
        except Exception as e:
            logger.error(f"Error in health monitoring: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f"Error: Health monitoring failed: {e}")

    async def fetch_wallet_assets(self, address: str) -> List[Dict]:
        """Fetch assets for a single wallet address"""
        async with self.blockfrost_session.get(f'/addresses/{address}/assets') as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Failed to get balance for {address}: {response.status}")

    async def fetch_multiple_wallet_details(self, wallets: List[str]) -> List[Dict]:
        """Fetch details for multiple wallets concurrently
        
        Args:
            wallets: List of wallet addresses to fetch
            
        Returns:
            List of wallet details from Blockfrost API
        """
        # Create tasks for each wallet request
        tasks = [self.fetch_wallet_assets(wallet) for wallet in wallets]
        
        # Execute all requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle any errors
        wallet_details = []
        for wallet, result in zip(wallets, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch details for wallet {wallet}: {result}")
                if self.admin_channel:
                    await self.admin_channel.send(f"Error: Failed to fetch details for wallet {wallet}: {result}")
                wallet_details.append({"address": wallet, "error": str(result)})
            else:
                wallet_details.append({"address": wallet, "assets": result})
                
        return wallet_details

    async def check_wallets_balance(self, wallets: List[str]) -> Dict[str, Any]:
        """Check balance changes for multiple wallets
        
        Args:
            wallets: List of wallet addresses to check
            
        Returns:
            Dict containing balance changes and notifications
        """
        try:
            # Fetch all wallet details concurrently
            current_balances = await self.fetch_multiple_wallet_details(wallets)
            
            changes = []
            notifications = []
            
            # Get previous balances from database
            async with self.pool.acquire() as conn:
                for wallet_data in current_balances:
                    address = wallet_data["address"]
                    
                    # Skip wallets that had errors
                    if "error" in wallet_data:
                        continue
                        
                    current_assets = wallet_data["assets"]
                    
                    # Get previous balance
                    previous = await conn.fetchrow(
                        'SELECT balance FROM wallet_balances WHERE address = $1 ORDER BY timestamp DESC LIMIT 1',
                        address
                    )
                    
                    if not previous:
                        # New wallet, store initial balance
                        await conn.execute(
                            'INSERT INTO wallet_balances (address, balance, timestamp) VALUES ($1, $2, $3)',
                            address, json.dumps(current_assets), datetime.now()
                        )
                        continue
                        
                    # Compare balances and detect changes
                    previous_assets = json.loads(previous['balance'])
                    balance_changes = self.compare_balances(previous_assets, current_assets)
                    
                    if balance_changes:
                        changes.append({
                            'address': address,
                            'changes': balance_changes
                        })
                        
                        # Store new balance
                        await conn.execute(
                            'INSERT INTO wallet_balances (address, balance, timestamp) VALUES ($1, $2, $3)',
                            address, json.dumps(current_assets), datetime.now()
                        )
                        
                        # Create notification
                        notification = self.create_balance_notification(address, balance_changes)
                        notifications.append(notification)
            
            return {
                'changes': changes,
                'notifications': notifications
            }
            
        except Exception as e:
            logger.error(f"Error checking wallet balances: {e}")
            if self.admin_channel:
                await self.admin_channel.send(f"Error checking wallet balances: {e}")
            raise

if __name__ == "__main__":
    try:
        # Configure logging for production
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Configure event loop policy for Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        # Create bot instance
        bot = WalletBudBot()

        async def main():
            try:
                # Create event loop
                loop = asyncio.get_running_loop()
                
                # Set up signal handlers for graceful shutdown
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(
                        bot.close()
                    ))
                
                try:
                    # Start the bot (this will call setup_hook)
                    await bot.start(DISCORD_TOKEN)
                    
                    # Start webhook server after bot is ready
                    await bot.start_webhook()
                    
                except Exception as e:
                    logger.error(f"Error during bot operation: {e}")
                    raise
                finally:
                    # Ensure cleanup happens
                    if not loop.is_closed():
                        await bot.close()
                        
            except Exception as e:
                logger.error(f"Error during bot initialization: {e}")
                raise

        # Run everything in the event loop
        asyncio.run(main())
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
