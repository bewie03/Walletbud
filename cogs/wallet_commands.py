import logging
import discord
from discord import app_commands
from discord.ext import commands, tasks
from typing import Optional
from database import (
    get_user_wallets,
    add_wallet,
    remove_wallet_for_user,
    get_notification_settings,
    update_notification_setting,
    get_wallet_for_user,
    get_yummi_warning_count,
    reset_yummi_warning,
    get_stake_address,
    update_stake_address
)
from decorators import dm_only, has_blockfrost, check_yummi_balance, command_cooldown
from cardano.address_validation import validate_cardano_address
from cachetools import TTLCache
from config import MINIMUM_YUMMI, COMMAND_COOLDOWN
from utils import format_ada_amount, format_token_amount
import os
import asyncio

logger = logging.getLogger(__name__)

class WalletCommands(commands.Cog):
    """Commands for managing Cardano wallets"""
    
    def __init__(self, bot):
        self.bot = bot
        # Cache for address balances (TTL of 5 minutes)
        self.balance_cache = TTLCache(maxsize=1000, ttl=300)
        # Initialize without requiring Blockfrost
        self.cleanup_cache.start()
        super().__init__()
        
    def cog_unload(self):
        self.cleanup_cache.cancel()

    @tasks.loop(minutes=5)
    async def cleanup_cache(self):
        """Cleanup expired cache entries"""
        self.balance_cache.expire()

    async def validate_address(self, address: str) -> bool:
        """Validate a Cardano address with proper error handling"""
        try:
            return validate_cardano_address(address)
        except Exception as e:
            logging.error(f"Address validation error: {e}")
            return False

    async def get_address_balance(self, address: str) -> dict:
        """Get address balance with caching and proper error handling"""
        try:
            # Check cache first
            if address in self.balance_cache:
                return self.balance_cache[address]

            # Use summary endpoint for efficiency
            summary = await self.bot.rate_limited_request(
                self.bot.blockfrost.addresses,
                address=address
            )
            
            balance_data = {
                'lovelace': summary.amount[0].quantity if summary.amount else 0,
                'tokens': {amt.unit: amt.quantity for amt in summary.amount[1:]} if len(summary.amount) > 1 else {}
            }
            
            # Cache the result
            self.balance_cache[address] = balance_data
            return balance_data
            
        except Exception as e:
            logging.error(f"Error fetching balance for {address}: {e}")
            raise

    @app_commands.command(name="add", description="Add a wallet to monitor")
    @commands.dm_only()
    @check_yummi_balance()
    @command_cooldown(COMMAND_COOLDOWN)
    async def add(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Validate address format
            if not await self.validate_address(address):
                await interaction.followup.send(
                    "❌ Invalid Cardano address format. Please check the address and try again.",
                    ephemeral=True
                )
                return

            # Check if address exists on chain
            try:
                await self.bot.rate_limited_request(
                    self.bot.blockfrost.addresses,
                    address=address
                )
            except Exception as e:
                await interaction.followup.send(
                    "❌ Could not verify address on the blockchain. Please check the address and try again.",
                    ephemeral=True
                )
                return

            # Add wallet to database
            success = await add_wallet(str(interaction.user.id), address)
            
            if success:
                await interaction.followup.send(
                    f"✅ Successfully added wallet `{address[:20]}...` to your monitored wallets!",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ This wallet is already being monitored or there was an error adding it.",
                    ephemeral=True
                )

        except Exception as e:
            logging.error(f"Error in add command: {e}")
            await interaction.followup.send(
                "❌ An error occurred while adding the wallet. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(name="remove", description="Remove a wallet from monitoring")
    async def remove(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Check if wallet exists
            try:
                wallets = await get_user_wallets(str(interaction.user.id))
                if address not in wallets:
                    await interaction.followup.send(
                        "❌ Wallet not found in your registered wallets.",
                        ephemeral=True
                    )
                    return
            except Exception as e:
                logger.error(f"Database error checking wallets: {e}")
                await interaction.followup.send(
                    "❌ Failed to check wallets. Please try again later.",
                    ephemeral=True
                )
                return
            
            # Remove wallet from database
            try:
                await remove_wallet_for_user(str(interaction.user.id), address)
                await interaction.followup.send(
                    f"✅ Successfully removed wallet from monitoring: `{address}`",
                    ephemeral=True
                )
            except Exception as e:
                logger.error(f"Database error removing wallet: {e}")
                await interaction.followup.send(
                    "❌ Failed to remove wallet. Please try again later.",
                    ephemeral=True
                )
                return
                
        except Exception as e:
            logger.error(f"Error in remove command: {e}")
            try:
                await interaction.followup.send(
                    "❌ An error occurred. Please try again later.",
                    ephemeral=True
                )
            except:
                pass

    @app_commands.command(name="list", description="List all registered wallets")
    async def list_wallets(self, interaction: discord.Interaction):
        """List all registered wallets"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Get user's wallets
            try:
                addresses = await get_user_wallets(str(interaction.user.id))
                
                # Check if user has any wallets
                if not addresses:
                    await interaction.followup.send(
                        "❌ You don't have any registered wallets! Use `/add` to add one.",
                        ephemeral=True
                    )
                    return
            except Exception as e:
                logger.error(f"Database error getting wallets: {e}")
                await interaction.followup.send(
                    "❌ Failed to get your wallets. Please try again later.",
                    ephemeral=True
                )
                return
            
            # Create embed
            embed = discord.Embed(
                title="📋 Your Registered Wallets",
                description=f"You have {len(addresses)} registered wallet{'s' if len(addresses) != 1 else ''}:",
                color=discord.Color.blue()
            )
            
            # Add field for each wallet
            for i, address in enumerate(addresses, 1):
                try:
                    # Get stake address
                    address_info = await self.bot.rate_limited_request(
                        self.bot.blockfrost.address,
                        address
                    )
                    stake_address = address_info.stake_address if address_info else None
                    
                    # Add field
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=(
                            f"**Address:** `{address[:8]}...{address[-8:]}`\n"
                            f"**Stake Address:** `{stake_address[:8]}...{stake_address[-8:] if stake_address else 'None'}`"
                        ),
                        inline=False
                    )
                except Exception as e:
                    logger.error(f"Error getting stake info for {address}: {e}")
                    embed.add_field(
                        name=f"Wallet {i}",
                        value=f"**Address:** `{address[:8]}...{address[-8:]}`\n**Stake Address:** `Error fetching`",
                        inline=False
                    )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error in list command: {e}")
            try:
                await interaction.followup.send(
                    "❌ An error occurred. Please try again later.",
                    ephemeral=True
                )
            except:
                pass

    @app_commands.command(name="balance", description="Get balances for all your monitored wallets")
    @commands.dm_only()
    @check_yummi_balance()
    @command_cooldown(COMMAND_COOLDOWN)
    async def balance(self, interaction: discord.Interaction):
        """Get balances for all monitored wallets"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Get user's wallets
            addresses = await get_user_wallets(str(interaction.user.id))
            
            if not addresses:
                await interaction.followup.send(
                    "❌ You don't have any registered wallets! Use `/add` to add one.",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title="💰 Your Wallet Balances",
                description=f"Showing balances for {len(addresses)} wallet{'s' if len(addresses) != 1 else ''}",
                color=discord.Color.blue()
            )

            total_ada = 0
            for address in addresses:
                try:
                    balance_data = await self.get_address_balance(address)
                    
                    # Calculate ADA balance
                    ada_balance = balance_data['lovelace'] / 1_000_000
                    total_ada += ada_balance
                    
                    # Format token balances
                    token_text = ""
                    if balance_data['tokens']:
                        token_list = [f"{quantity} {unit}" for unit, quantity in balance_data['tokens'].items()]
                        token_text = f"\nTokens: {', '.join(token_list[:3])}"
                        if len(token_list) > 3:
                            token_text += f" (+{len(token_list)-3} more)"
                    
                    embed.add_field(
                        name=f"Address: {address[:20]}...",
                        value=f"Balance: {ada_balance:.2f} ADA{token_text}",
                        inline=False
                    )
                    
                except Exception as e:
                    embed.add_field(
                        name=f"Address: {address[:20]}...",
                        value="❌ Error fetching balance",
                        inline=False
                    )
                    logging.error(f"Error fetching balance for {address}: {e}")
            
            embed.set_footer(text=f"Total ADA: {total_ada:.2f}")
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logging.error(f"Error in balance command: {e}")
            await interaction.followup.send(
                "❌ An error occurred while fetching balances. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    """Set up the WalletCommands cog"""
    await bot.add_cog(WalletCommands(bot))
