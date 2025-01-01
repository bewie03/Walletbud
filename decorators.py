import logging
import discord
from discord import app_commands
from functools import wraps
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict

logger = logging.getLogger(__name__)

def dm_only():
    """Decorator to restrict commands to DMs only"""
    async def predicate(interaction: discord.Interaction):
        if not isinstance(interaction.channel, discord.DMChannel):
            await interaction.response.send_message(
                "❌ This command can only be used in DMs!",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

def has_blockfrost(func=None):
    """Decorator to check if Blockfrost client is available"""
    async def predicate(interaction: discord.Interaction):
        if not hasattr(interaction.client, 'blockfrost') or interaction.client.blockfrost is None:
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            return False
        return True
        
    if func is None:
        return app_commands.check(predicate)
    return app_commands.check(predicate)(func)

def command_cooldown(seconds: int = 60):
    """Decorator to add cooldown to commands"""
    def decorator(func):
        cooldowns: Dict[int, datetime] = {}
        
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            user_id = interaction.user.id
            now = datetime.now()
            
            if user_id in cooldowns:
                time_left = (cooldowns[user_id] - now).total_seconds()
                if time_left > 0:
                    await interaction.response.send_message(
                        f"⏳ Please wait {int(time_left)} seconds before using this command again.",
                        ephemeral=True
                    )
                    return
                    
            cooldowns[user_id] = now + timedelta(seconds=seconds)
            return await func(self, interaction, *args, **kwargs)
            
        return wrapper
    return decorator

def check_yummi_balance():
    """Decorator to check if user has sufficient YUMMI balance"""
    async def predicate(interaction: discord.Interaction):
        if not hasattr(interaction.client, 'check_yummi_requirement'):
            logger.error("Bot instance missing check_yummi_requirement method")
            return False
            
        user_id = str(interaction.user.id)
        wallets = await interaction.client.get_user_wallets(user_id)
        
        if not wallets:
            await interaction.response.send_message(
                "❌ You don't have any registered wallets. Use `/add` to add a wallet.",
                ephemeral=True
            )
            return False
            
        for wallet in wallets:
            if await interaction.client.check_yummi_requirement(wallet['address'], user_id):
                return True
                
        await interaction.response.send_message(
            f"❌ You need at least {interaction.client.MINIMUM_YUMMI} YUMMI tokens in one of your wallets to use this command.",
            ephemeral=True
        )
        return False
        
    return app_commands.check(predicate)
