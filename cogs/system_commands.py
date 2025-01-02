import os
import json
import logging
import asyncio
import discord
import traceback
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime
import psutil
from typing import Optional, Dict, Any
from cachetools import TTLCache
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
    HEALTH_CHECK_INTERVAL,
    HEALTH_METRICS_TTL
)
import aiofiles

# Initialize logger with proper levels
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVELS.get('system_commands', logging.INFO))

class SystemCommands(commands.Cog):
    """System and utility commands"""
    
    def __init__(self, bot):
        self.bot = bot
        self._health_lock = asyncio.Lock()
        # Use TTLCache for health metrics
        self._health_cache = TTLCache(maxsize=1, ttl=HEALTH_METRICS_TTL)
        self.health_monitor.start()
        super().__init__()

    def cog_unload(self):
        """Clean up tasks on unload"""
        self.health_monitor.cancel()

    @tasks.loop(seconds=HEALTH_CHECK_INTERVAL)
    async def health_monitor(self):
        """Background task to monitor system health"""
        try:
            async with self._health_lock:
                health_data = await self.bot.health_check()
                
                # Update cache
                self._health_cache['data'] = health_data
                
                # Log issues if status is not healthy
                if health_data['status'] != 'healthy':
                    logger.warning(f"Health check reported status: {health_data['status']}")
                    for component, data in health_data['components'].items():
                        if not data.get('healthy', False):
                            logger.warning(f"Unhealthy component {component}: {data.get('error', 'No error details')}")
                
                # Alert admin channel if configured
                if (
                    health_data['status'] == 'unhealthy' and 
                    hasattr(self.bot, 'admin_channel') and 
                    self.bot.admin_channel
                ):
                    embed = discord.Embed(
                        title="⚠️ System Health Alert",
                        description=f"Status: {health_data['status']}",
                        color=discord.Color.red()
                    )
                    for component, data in health_data['components'].items():
                        if not data.get('healthy', False):
                            embed.add_field(
                                name=f"❌ {component}",
                                value=data.get('error', 'No error details'),
                                inline=False
                            )
                    try:
                        await self.bot.admin_channel.send(embed=embed)
                    except Exception as e:
                        logger.error(f"Failed to send health alert: {e}")
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error in health monitor: {e}", exc_info=True)

    async def _check_health_metrics(self) -> Dict[str, Any]:
        """Gather all health metrics"""
        try:
            # Try to get from cache first
            if 'data' in self._health_cache:
                return self._health_cache['data']
            
            # Get fresh data if not in cache
            async with self._health_lock:
                health_data = await self.bot.health_check()
                self._health_cache['data'] = health_data
                return health_data
                
        except Exception as e:
            logger.error(f"Error checking health metrics: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'error': str(e),
                'components': {}
            }

    @app_commands.command(name="health", description="Check bot and API status")
    @command_cooldown(COMMAND_COOLDOWN)
    async def health(self, interaction: discord.Interaction):
        """Show detailed system health status"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Get health data
            health_data = await self._check_health_metrics()
            
            # Create health status embed
            status = health_data['status']
            color = {
                'healthy': discord.Color.green(),
                'degraded': discord.Color.orange(),
                'unhealthy': discord.Color.red()
            }.get(status, discord.Color.greyple())
            
            embed = discord.Embed(
                title="🏥 System Health Status",
                description=(
                    f"Overall Status: {status.title()}\n"
                    f"{'✅' if status == 'healthy' else '⚠️' if status == 'degraded' else '❌'}"
                ),
                color=color
            )
            
            # Add component status fields
            for component, data in health_data['components'].items():
                healthy = data.get('healthy', False)
                field_value = [f"Status: {'✅' if healthy else '❌'}"]
                
                # Add component-specific metrics
                if component == 'discord':
                    field_value.extend([
                        f"Latency: {data['latency']}ms",
                        f"Guilds: {data['guilds']}",
                        f"Shards: {data['shards']}"
                    ])
                elif component == 'blockfrost' and healthy:
                    field_value.extend([
                        f"Network: {data['network']}",
                        f"Sync: {data['sync_progress']}%"
                    ])
                    if 'rate_limits' in data:
                        field_value.append(
                            f"Rate Limits: {data['rate_limits']['remaining']}/{data['rate_limits']['total']}"
                        )
                elif component == 'database' and healthy:
                    field_value.extend([
                        f"Pool: {data['used_connections']}/{data['max_size']}",
                        f"Min Size: {data['min_size']}"
                    ])
                elif component == 'system' and healthy:
                    field_value.extend([
                        f"CPU: {data['cpu_percent']}%",
                        f"Memory: {data['memory_percent']}%",
                        f"Uptime: {data['uptime']}"
                    ])
                
                embed.add_field(
                    name=f"{component.title()}",
                    value="\n".join(field_value),
                    inline=True
                )
            
            # Add error details if present
            if 'error' in health_data:
                embed.add_field(
                    name="❌ Error Details",
                    value=f"```{health_data['error']}```",
                    inline=False
                )
            
            # Add timestamp
            embed.set_footer(text=f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error showing health status: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while checking health status. Please try again later.",
                ephemeral=True
            )

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

    @app_commands.command(name="fallback")
    @app_commands.default_permissions(administrator=True)
    async def toggle_fallback(self, interaction: discord.Interaction):
        """Toggle fallback mode for basic functionality when Blockfrost is down"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            self.bot.fallback_mode = not getattr(self.bot, 'fallback_mode', False)
            mode = "enabled" if self.bot.fallback_mode else "disabled"
            
            # Update command availability
            await self.bot.tree.sync()
            
            await interaction.followup.send(
                f"✅ Fallback mode {mode}. Basic commands will {'work without' if self.bot.fallback_mode else 'require'} Blockfrost.",
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Failed to toggle fallback mode: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ Failed to toggle fallback mode. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="logs")
    @app_commands.default_permissions(administrator=True)
    async def get_logs(self, interaction: discord.Interaction, lines: int = 50):
        """Get recent log entries"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Read last N lines from log file
            async with aiofiles.open('bot.log', 'r') as f:
                content = await f.read()
                log_lines = content.splitlines()[-lines:]
            
            # Format logs into chunks (Discord has a 2000 char limit)
            chunks = []
            current_chunk = []
            current_length = 0
            
            for line in log_lines:
                if current_length + len(line) + 2 > 1900:  # Leave some margin
                    chunks.append('\n'.join(current_chunk))
                    current_chunk = []
                    current_length = 0
                
                current_chunk.append(line)
                current_length += len(line) + 2  # +2 for newline
            
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
            
            # Send logs in multiple messages if needed
            for i, chunk in enumerate(chunks):
                await interaction.followup.send(
                    f"```\n{chunk}\n```",
                    ephemeral=True
                )
            
        except Exception as e:
            logger.error(f"Failed to get logs: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ Failed to retrieve logs. Please check the server.",
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
    cog = SystemCommands(bot)
    await bot.add_cog(cog)
