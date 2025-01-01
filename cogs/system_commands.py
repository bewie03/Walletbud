import logging
import discord
from discord import app_commands
from discord.ext import commands, tasks
from datetime import datetime
import psutil
from typing import Optional, Dict, Any
from database import (
    get_notification_settings,
    update_notification_setting,
    get_user_wallets,
    initialize_notification_settings
)
from decorators import dm_only, has_blockfrost, command_cooldown
from config import (
    NOTIFICATION_SETTINGS,
    COMMAND_COOLDOWN,
    LOG_LEVELS,
    EMBED_CHAR_LIMIT,
    HEALTH_CHECK_INTERVAL
)

# Initialize logger with proper levels
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVELS.get('system_commands', logging.INFO))

class SystemCommands(commands.Cog):
    """System and utility commands"""
    
    def __init__(self, bot):
        self.bot = bot
        self._last_health_check = None
        self._health_cache = {}
        self.health_monitor.start()
        super().__init__()

    def cog_unload(self):
        """Clean up tasks on unload"""
        self.health_monitor.cancel()

    @tasks.loop(seconds=HEALTH_CHECK_INTERVAL)
    async def health_monitor(self):
        """Background task to monitor system health"""
        try:
            health_data = await self._check_health_metrics()
            self._health_cache = health_data
            self._last_health_check = datetime.now()
            
            # Log any concerning metrics
            if not health_data['blockfrost']['healthy']:
                logger.error("Blockfrost API health check failed")
            if health_data['system']['memory_percent'] > 90:
                logger.warning("High memory usage detected")
            if health_data['system']['cpu_percent'] > 80:
                logger.warning("High CPU usage detected")
                
        except Exception as e:
            logger.error(f"Health monitor error: {e}", exc_info=True)

    async def _check_health_metrics(self) -> Dict[str, Any]:
        """Gather all health metrics"""
        health_data = {
            'discord': {
                'connected': self.bot.is_ready(),
                'latency': round(self.bot.latency * 1000)
            },
            'database': {'connected': False, 'error': None},
            'blockfrost': {'healthy': False, 'error': None},
            'system': {},
            'rate_limits': {
                'blockfrost': {
                    'remaining': 0,
                    'reset_at': None,
                    'total': 0
                },
                'discord': {
                    'global_rate_limit': False,
                    'command_rate_limits': {}
                }
            }
        }
        
        # Check database
        try:
            if hasattr(self.bot, 'pool') and self.bot.pool:
                async with self.bot.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                health_data['database']['connected'] = True
        except Exception as e:
            health_data['database']['error'] = str(e)
            logger.error(f"Database health check failed: {e}", exc_info=True)

        # Check Blockfrost with rate limits
        try:
            if hasattr(self.bot, 'blockfrost') and self.bot.blockfrost:
                health = await self.bot.blockfrost.health()
                health_data['blockfrost']['healthy'] = health and health.is_healthy
                
                # Get rate limit info if available
                if hasattr(self.bot.blockfrost, 'get_rate_limits'):
                    rate_limits = await self.bot.blockfrost.get_rate_limits()
                    health_data['rate_limits']['blockfrost'] = {
                        'remaining': rate_limits.remaining,
                        'reset_at': rate_limits.reset_at,
                        'total': rate_limits.total
                    }
        except Exception as e:
            health_data['blockfrost']['error'] = str(e)
            logger.error(f"Blockfrost health check failed: {e}", exc_info=True)

        # System metrics with more detail
        try:
            process = psutil.Process()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            health_data['system'] = {
                'memory_used': process.memory_info().rss / 1024 / 1024,
                'memory_percent': process.memory_percent(),
                'system_memory': {
                    'total': memory.total / (1024 * 1024 * 1024),
                    'available': memory.available / (1024 * 1024 * 1024),
                    'percent': memory.percent
                },
                'cpu_percent': process.cpu_percent(),
                'cpu_count': psutil.cpu_count(),
                'disk_usage': {
                    'total': disk.total / (1024 * 1024 * 1024),
                    'used': disk.used / (1024 * 1024 * 1024),
                    'free': disk.free / (1024 * 1024 * 1024),
                    'percent': disk.percent
                },
                'uptime': datetime.now() - self.bot.start_time if hasattr(self.bot, 'start_time') else None,
                'thread_count': len(process.threads()),
                'open_files': len(process.open_files())
            }
        except Exception as e:
            logger.error(f"System metrics check failed: {e}", exc_info=True)
            
        return health_data

    def _create_paginated_embed(self, title: str, fields: list, description: str = "") -> list[discord.Embed]:
        """Create paginated embeds if content exceeds Discord's limits"""
        embeds = []
        current_embed = discord.Embed(title=title, description=description, color=discord.Color.blue())
        current_length = len(title) + len(description)
        
        for field in fields:
            field_length = len(field['name']) + len(field['value'])
            if current_length + field_length > EMBED_CHAR_LIMIT:
                embeds.append(current_embed)
                current_embed = discord.Embed(title=f"{title} (cont.)", color=discord.Color.blue())
                current_length = len(title) + 7  # "(cont.)"
                
            current_embed.add_field(**field)
            current_length += field_length
            
        embeds.append(current_embed)
        return embeds

    @app_commands.command(name="help", description="Show bot help and commands")
    @command_cooldown(COMMAND_COOLDOWN)
    async def help(self, interaction: discord.Interaction):
        """Show bot help and commands"""
        try:
            # Dynamically gather commands
            commands = []
            for command in self.bot.tree.walk_commands():
                if isinstance(command, app_commands.Command):
                    commands.append({
                        'name': command.name,
                        'description': command.description,
                        'category': command.extras.get('category', 'Miscellaneous')
                    })

            # Group commands by category
            categories = {}
            for cmd in commands:
                if cmd['category'] not in categories:
                    categories[cmd['category']] = []
                categories[cmd['category']].append(cmd)

            # Create embed fields
            fields = []
            for category, cmds in categories.items():
                fields.append({
                    'name': f"📌 {category}",
                    'value': "\n".join([f"`/{cmd['name']}` - {cmd['description']}" for cmd in cmds]),
                    'inline': False
                })

            # Add requirements and notification types
            fields.extend([
                {
                    'name': "📋 Requirements",
                    'value': (
                        "To use WalletBud, you need:\n"
                        "• A Cardano wallet address\n"
                        "• Required YUMMI tokens\n"
                        "• Sufficient ADA for fees"
                    ),
                    'inline': False
                },
                {
                    'name': "🔔 Notification Types",
                    'value': "\n".join([f"• {name}" for name in NOTIFICATION_SETTINGS.keys()]),
                    'inline': False
                }
            ])

            # Create paginated embeds
            embeds = self._create_paginated_embed(
                "🤖 WalletBud Help",
                fields,
                "Welcome to WalletBud! Here are the available commands:"
            )

            # Send embeds
            for i, embed in enumerate(embeds):
                if i == 0:
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                else:
                    await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error showing help: {e}", exc_info=True)
            await interaction.response.send_message(
                "❌ An error occurred while showing help. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="health", description="Check bot and API status")
    @command_cooldown(COMMAND_COOLDOWN)
    async def health(self, interaction: discord.Interaction):
        """Check bot and API status"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Get health metrics
            health_data = await self._check_health_metrics()
            
            embed = discord.Embed(
                title="🏥 System Health Check",
                color=discord.Color.blue()
            )
            
            # Discord status
            embed.add_field(
                name="Discord Connection",
                value=(
                    f"{'✅' if health_data['discord']['connected'] else '❌'} Connected\n"
                    f"Latency: {health_data['discord']['latency']}ms"
                ),
                inline=False
            )
            
            # Database status
            status = "✅ Connected" if health_data['database']['connected'] else f"❌ Error: {health_data['database']['error']}"
            embed.add_field(
                name="Database",
                value=status,
                inline=False
            )
            
            # Blockfrost status with rate limits
            blockfrost_status = []
            if health_data['blockfrost']['healthy']:
                blockfrost_status.append("✅ Healthy")
            else:
                blockfrost_status.append(f"❌ Error: {health_data['blockfrost']['error']}")
            
            rate_limits = health_data['rate_limits']['blockfrost']
            if rate_limits['total'] > 0:
                blockfrost_status.append(
                    f"Rate Limits: {rate_limits['remaining']}/{rate_limits['total']} remaining\n"
                    f"Reset at: {rate_limits['reset_at']}"
                )
            
            embed.add_field(
                name="Blockfrost API",
                value="\n".join(blockfrost_status),
                inline=False
            )
            
            # System metrics with more detail
            system = health_data['system']
            embed.add_field(
                name="System Metrics",
                value=(
                    f"Memory Usage: {system['memory_used']:.1f} MB ({system['memory_percent']:.1f}%)\n"
                    f"System Memory: {system['system_memory']['available']:.1f}GB free of {system['system_memory']['total']:.1f}GB\n"
                    f"CPU Usage: {system['cpu_percent']}% across {system['cpu_count']} cores\n"
                    f"Disk Space: {system['disk_usage']['free']:.1f}GB free of {system['disk_usage']['total']:.1f}GB\n"
                    f"Threads: {system['thread_count']}\n"
                    f"Open Files: {system['open_files']}\n"
                    f"Uptime: {system['uptime']}"
                ),
                inline=False
            )
            
            # Add warnings for concerning metrics
            warnings = []
            if system['memory_percent'] > 80:
                warnings.append("⚠️ High memory usage")
            if system['cpu_percent'] > 70:
                warnings.append("⚠️ High CPU usage")
            if system['disk_usage']['percent'] > 90:
                warnings.append("⚠️ Low disk space")
            if rate_limits['remaining'] < rate_limits['total'] * 0.2:
                warnings.append("⚠️ API rate limit running low")
            
            if warnings:
                embed.add_field(
                    name="⚠️ Warnings",
                    value="\n".join(warnings),
                    inline=False
                )
            
            # Add troubleshooting tips if there are issues
            if warnings or not health_data['blockfrost']['healthy'] or not health_data['database']['connected']:
                embed.add_field(
                    name="🔧 Troubleshooting Steps",
                    value=(
                        "1. Check your internet connection\n"
                        "2. Verify API credentials are correct\n"
                        "3. Check service status pages\n"
                        "4. Consider upgrading resources if system metrics are high\n"
                        "5. Contact support if issues persist"
                    ),
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error in health command: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while checking system health. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="notifications", description="View your notification settings")
    @commands.dm_only()
    @command_cooldown(COMMAND_COOLDOWN)
    async def notifications(self, interaction: discord.Interaction):
        """View your notification settings"""
        try:
            settings = await get_notification_settings(str(interaction.user.id))
            
            # Don't save default settings automatically
            if not settings:
                settings = NOTIFICATION_SETTINGS.copy()
            
            # Create embed fields
            fields = []
            for setting, config in NOTIFICATION_SETTINGS.items():
                enabled = settings.get(setting, config['default'])
                fields.append({
                    'name': config['display_name'],
                    'value': (
                        f"{'✅' if enabled else '❌'} "
                        f"{config['description']}\n"
                        f"Default: {'Enabled' if config['default'] else 'Disabled'}"
                    ),
                    'inline': True
                })
            
            # Add instructions
            fields.append({
                'name': "ℹ️ How to Change Settings",
                'value': "Use `/toggle <setting> <enabled>` to change a setting.\nExample: `/toggle ada_transactions false`",
                'inline': False
            })
            
            # Create paginated embeds
            embeds = self._create_paginated_embed(
                "🔔 Notification Settings",
                fields,
                "Here are your current notification settings:"
            )
            
            # Send embeds
            for i, embed in enumerate(embeds):
                if i == 0:
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                else:
                    await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing notification settings: {e}", exc_info=True)
            await interaction.response.send_message(
                "❌ An error occurred while retrieving notification settings. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="toggle", description="Toggle notification settings")
    @commands.dm_only()
    @command_cooldown(COMMAND_COOLDOWN)
    async def toggle(self, interaction: discord.Interaction, setting: str, enabled: bool):
        """Toggle notification settings"""
        try:
            # Validate setting
            if setting not in NOTIFICATION_SETTINGS:
                valid_settings = "\n".join([f"• {s}" for s in NOTIFICATION_SETTINGS.keys()])
                await interaction.response.send_message(
                    f"❌ Invalid setting. Available settings:\n{valid_settings}",
                    ephemeral=True
                )
                return
            
            # Get current settings
            settings = await get_notification_settings(str(interaction.user.id))
            if not settings:
                settings = NOTIFICATION_SETTINGS.copy()
            
            # Validate against schema
            if not await validate_notification_schema(setting, enabled):
                await interaction.response.send_message(
                    "❌ Invalid setting value. Please check the setting type and try again.",
                    ephemeral=True
                )
                return
            
            # Update setting
            success = await update_notification_setting(
                str(interaction.user.id),
                setting,
                enabled
            )
            
            if success:
                config = NOTIFICATION_SETTINGS[setting]
                await interaction.response.send_message(
                    f"✅ {config['display_name']} notifications are now "
                    f"{'enabled' if enabled else 'disabled'}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ Failed to update notification settings. Please try again later.",
                    ephemeral=True
                )
            
        except Exception as e:
            logger.error(f"Error toggling notification setting: {e}", exc_info=True)
            await interaction.response.send_message(
                "❌ An error occurred while updating notification settings. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    """Set up the SystemCommands cog"""
    await bot.add_cog(SystemCommands(bot))
