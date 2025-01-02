import os
import json
import logging
import asyncio
import discord
import traceback
from functools import wraps
from datetime import datetime, timedelta
from typing import Dict, Any, Callable, Awaitable, TypeVar, Optional, List

from config import (
    MINIMUM_YUMMI,
    COMMAND_COOLDOWN as DEFAULT_COOLDOWN,
    RATE_LIMIT_COOLDOWN,
    MAX_RETRIES
)

logger = logging.getLogger(__name__)

def handle_errors():
    """Decorator for comprehensive error handling in commands"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            try:
                return await func(self, interaction, *args, **kwargs)
            except asyncio.TimeoutError:
                logger.error(f"Timeout in {func.__name__}:\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "The operation timed out. Please try again.",
                    ephemeral=True
                )
            except discord.errors.Forbidden as e:
                logger.error(f"Permission error in {func.__name__}: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "I don't have permission to perform this action.",
                    ephemeral=True
                )
            except discord.errors.NotFound as e:
                logger.error(f"Resource not found in {func.__name__}: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "The requested resource was not found.",
                    ephemeral=True
                )
            except discord.errors.HTTPException as e:
                logger.error(f"Discord API error in {func.__name__}: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "There was an error communicating with Discord. Please try again.",
                    ephemeral=True
                )
            except ValueError as e:
                logger.error(f"Validation error in {func.__name__}: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    str(e) or "Invalid input provided.",
                    ephemeral=True
                )
            except Exception as e:
                logger.error(f"Unexpected error in {func.__name__}: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "An unexpected error occurred. Please try again later.",
                    ephemeral=True
                )
        return wrapper
    return decorator

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
    """Decorator to check if Blockfrost client is available and healthy"""
    async def predicate(interaction: discord.Interaction):
        if not hasattr(interaction.client, 'blockfrost') or interaction.client.blockfrost is None:
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            if hasattr(interaction.client, 'admin_channel') and interaction.client.admin_channel:
                await interaction.client.admin_channel.send("🚨 ERROR: Command failed: Blockfrost client not initialized")
            return False
            
        # Check if Blockfrost is responding with rate limiting and timeouts
        try:
            async with interaction.client.rate_limiter.acquire("blockfrost"):
                # Use a shorter timeout for health check
                async with asyncio.timeout(5):
                    await interaction.client.blockfrost.health()
                    interaction.client.health_metrics['last_api_call'] = datetime.utcnow()
                    return True
                    
        except asyncio.TimeoutError:
            logger.error("Blockfrost health check timed out")
            await interaction.response.send_message(
                "❌ Blockfrost API is not responding. Please try again later.",
                ephemeral=True
            )
            if hasattr(interaction.client, 'admin_channel') and interaction.client.admin_channel:
                await interaction.client.admin_channel.send("⚠️ WARNING: Blockfrost health check timed out")
            return False
            
        except Exception as e:
            logger.error(f"Blockfrost health check failed: {e}")
            await interaction.response.send_message(
                "❌ Blockfrost API is not available. Please try again later.",
                ephemeral=True
            )
            if hasattr(interaction.client, 'admin_channel') and interaction.client.admin_channel:
                await interaction.client.admin_channel.send(f"🚨 ERROR: Blockfrost health check failed: {e}")
            return False
        
    if func is None:
        return app_commands.check(predicate)
    return app_commands.check(predicate)(func)

def command_cooldown(seconds: int = DEFAULT_COOLDOWN):
    """Decorator to add cooldown to commands with proper cleanup"""
    def decorator(func: Callable):
        cooldowns: Dict[int, datetime] = {}
        
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            now = datetime.utcnow()
            user_id = interaction.user.id
            
            # Clean up expired cooldowns
            expired = [uid for uid, time in cooldowns.items() 
                      if now - time > timedelta(seconds=seconds)]
            for uid in expired:
                del cooldowns[uid]
            
            # Check if user is on cooldown
            if user_id in cooldowns:
                remaining = (cooldowns[user_id] + timedelta(seconds=seconds) - now).total_seconds()
                if remaining > 0:
                    await interaction.response.send_message(
                        f"This command is on cooldown. Please wait {int(remaining)} seconds.",
                        ephemeral=True
                    )
                    return
            
            # Execute command and update cooldown
            try:
                result = await func(self, interaction, *args, **kwargs)
                cooldowns[user_id] = now
                return result
            except Exception as e:
                # Don't apply cooldown if command failed
                logger.error(f"Error in {func.__name__}: {e}\n{traceback.format_exc()}")
                raise
                
        return wrapper
    return decorator

def check_yummi_balance(min_balance: Optional[int] = None):
    """Decorator to check if user has sufficient YUMMI balance with proper error handling"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            try:
                # Get user's wallets
                wallets = await self.get_user_wallets(interaction.user.id)
                if not wallets:
                    await interaction.response.send_message(
                        "❌ You need to add a wallet first!",
                        ephemeral=True
                    )
                    return
                
                # Check YUMMI balance for each wallet individually
                required = min_balance or self.MINIMUM_YUMMI
                insufficient_wallets = []
                
                for wallet in wallets:
                    balance = await self.get_token_balance(
                        wallet['address'],
                        self.YUMMI_POLICY_ID,
                        self.YUMMI_ASSET_NAME
                    ) or 0
                    
                    if balance < required:
                        insufficient_wallets.append({
                            'address': wallet['address'],
                            'balance': balance
                        })
                
                if insufficient_wallets:
                    # Create detailed message about insufficient balances
                    message = "❌ Insufficient YUMMI balance in the following wallets:\n"
                    for wallet in insufficient_wallets:
                        message += f"• `{wallet['address']}`: {wallet['balance']:,} YUMMI (Need {required:,})\n"
                    message += f"\nEach wallet must hold at least {required:,} YUMMI to use this feature."
                    
                    await interaction.response.send_message(
                        message,
                        ephemeral=True
                    )
                    return
                
                return await func(self, interaction, *args, **kwargs)
                
            except Exception as e:
                logger.error(f"Error in check_yummi_balance: {e}")
                await interaction.response.send_message(
                    "❌ Error checking YUMMI balance. Please try again later.",
                    ephemeral=True
                )
                return
                
        return wrapper
    return decorator

def validate_wallet_address():
    """Decorator to validate Cardano wallet address format"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, address: str, *args, **kwargs):
            # Basic address format validation
            if not address.startswith(('addr1', 'stake1')):
                await interaction.response.send_message(
                    "❌ Invalid Cardano address format. Address must start with 'addr1' or 'stake1'.",
                    ephemeral=True
                )
                return
                
            # Length validation
            if not (len(address) >= 98 and len(address) <= 110):
                await interaction.response.send_message(
                    "❌ Invalid address length.",
                    ephemeral=True
                )
                return
                
            # Character set validation
            if not all(c in '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' for c in address[5:]):
                await interaction.response.send_message(
                    "❌ Address contains invalid characters.",
                    ephemeral=True
                )
                return
                
            return await func(self, interaction, address, *args, **kwargs)
            
        return wrapper
    return decorator
