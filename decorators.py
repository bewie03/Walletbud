import logging
import discord
from discord import app_commands
from functools import wraps
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable
import traceback

logger = logging.getLogger(__name__)

def handle_errors():
    """Decorator for comprehensive error handling in commands"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            try:
                return await func(self, interaction, *args, **kwargs)
            except asyncio.TimeoutError:
                await interaction.followup.send(
                    "❌ The operation timed out. Please try again.",
                    ephemeral=True
                )
                logger.error(f"Timeout in {func.__name__}: {traceback.format_exc()}")
            except discord.errors.NotFound:
                await interaction.followup.send(
                    "❌ The interaction expired. Please try the command again.",
                    ephemeral=True
                )
            except discord.errors.HTTPException as e:
                await interaction.followup.send(
                    "❌ There was an error processing your request. Please try again later.",
                    ephemeral=True
                )
                logger.error(f"HTTP error in {func.__name__}: {e}\n{traceback.format_exc()}")
            except Exception as e:
                await interaction.followup.send(
                    "❌ An unexpected error occurred. The issue has been logged.",
                    ephemeral=True
                )
                logger.error(f"Unexpected error in {func.__name__}: {e}\n{traceback.format_exc()}")
                if hasattr(self, 'send_admin_alert'):
                    await self.send_admin_alert(
                        f"Error in {func.__name__}:\n```{traceback.format_exc()}```"
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
            if hasattr(interaction.client, 'send_admin_alert'):
                await interaction.client.send_admin_alert("🚨 Command failed: Blockfrost client not initialized")
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
            if hasattr(interaction.client, 'send_admin_alert'):
                await interaction.client.send_admin_alert("⚠️ Blockfrost health check timed out")
            return False
            
        except Exception as e:
            logger.error(f"Blockfrost health check failed: {e}")
            await interaction.response.send_message(
                "❌ Blockfrost API is experiencing issues. Please try again later.",
                ephemeral=True
            )
            if hasattr(interaction.client, 'send_admin_alert'):
                await interaction.client.send_admin_alert(f"⚠️ Blockfrost health check failed: {str(e)}")
            return False
        
    if func is None:
        return app_commands.check(predicate)
    return app_commands.check(predicate)(func)

def command_cooldown(seconds: int = 60):
    """Decorator to add cooldown to commands with proper cleanup"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, interaction: discord.Interaction, *args, **kwargs):
            # Get user's cooldown key
            cooldown_key = f"{func.__name__}:{interaction.user.id}"
            
            # Check if user is in cooldown
            if cooldown_key in self.interaction_cooldowns:
                remaining = self.interaction_cooldowns[cooldown_key] - datetime.utcnow()
                if remaining.total_seconds() > 0:
                    await interaction.response.send_message(
                        f"⏳ Please wait {int(remaining.total_seconds())} seconds before using this command again.",
                        ephemeral=True
                    )
                    return
                    
            try:
                # Set cooldown before executing command
                self.interaction_cooldowns[cooldown_key] = datetime.utcnow() + timedelta(seconds=seconds)
                
                # Execute command
                result = await func(self, interaction, *args, **kwargs)
                return result
                
            except Exception as e:
                logger.error(f"Error in command {func.__name__}: {e}")
                raise
                
            finally:
                # Clean up cooldown after command execution or error
                try:
                    del self.interaction_cooldowns[cooldown_key]
                except KeyError:
                    pass
                    
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
                
                # Check YUMMI balance across all wallets
                total_yummi = 0
                for wallet in wallets:
                    balance = await self.get_token_balance(
                        wallet['address'],
                        self.YUMMI_POLICY_ID,
                        self.YUMMI_ASSET_NAME
                    )
                    total_yummi += balance or 0
                
                required = min_balance or self.MINIMUM_YUMMI
                if total_yummi < required:
                    await interaction.response.send_message(
                        f"❌ You need at least {required:,} YUMMI tokens to use this command. "
                        f"Current balance: {total_yummi:,} YUMMI",
                        ephemeral=True
                    )
                    return
                
                return await func(self, interaction, *args, **kwargs)
                
            except Exception as e:
                logger.error(f"Error checking YUMMI balance: {e}\n{traceback.format_exc()}")
                await interaction.response.send_message(
                    "❌ Error checking YUMMI balance. Please try again later.",
                    ephemeral=True
                )
                
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
