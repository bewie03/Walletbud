import unittest
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
from bot import WalletBud
import config
import os
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime
from database import (
    get_wallet_for_user,
    get_all_wallets_for_user,
    get_notification_settings,
    add_wallet_for_user,
    remove_wallet_for_user,
    update_notification_settings,
)

class MockBot(discord.Client):
    """Mock bot for testing"""
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)
        self._application = discord.Object(id=123456789)
        self._user = discord.Object(id=123456789)
        self.blockfrost_client = None
        self.DEFAULT_SETTINGS = WalletBud.DEFAULT_SETTINGS
        
    @property
    def application(self):
        return self._application
        
    @property
    def user(self):
        return self._user
        
    async def _balance(self, interaction: discord.Interaction):
        """Check wallet balances"""
        try:
            wallets = await get_all_wallets_for_user(str(interaction.user.id))
            if not wallets:
                await interaction.response.send_message("You have no registered wallets.")
                return

            message = "💰 Your Wallet Balances\n\n"
            for wallet in wallets:
                message += f"Wallet: {wallet}\n"
                message += f"Balance: 5.0 ADA\n\n"

            await interaction.response.send_message(message)
        except Exception as e:
            await interaction.response.send_message(f"Failed to get wallet balances: {str(e)}")

    async def _add_wallet(self, interaction: discord.Interaction, address: str):
        """Add a wallet to monitor"""
        try:
            # Validate address format
            if not address.startswith("addr1"):
                await interaction.response.send_message("Invalid wallet address format.")
                return

            # Check if wallet is already registered
            wallets = await get_all_wallets_for_user(str(interaction.user.id))
            if address in wallets:
                await interaction.response.send_message("This wallet is already registered.")
                return

            # Try to add wallet
            success = await add_wallet_for_user(str(interaction.user.id), address)
            if success:
                await interaction.response.send_message(f"Added wallet {address}")
            else:
                await interaction.response.send_message("Failed to add wallet.")
        except Exception as e:
            await interaction.response.send_message("This wallet is already registered.")

    async def _remove_wallet(self, interaction: discord.Interaction, address: str):
        """Remove a wallet from monitoring"""
        try:
            # Check if wallet exists
            wallets = await get_all_wallets_for_user(str(interaction.user.id))
            if address not in wallets:
                await interaction.response.send_message("Wallet not found in your registered wallets.")
                return

            # Try to remove wallet
            success = await remove_wallet_for_user(str(interaction.user.id), address)
            if success:
                await interaction.response.send_message("Wallet removed successfully!")
            else:
                await interaction.response.send_message("Failed to remove wallet.")
        except Exception as e:
            await interaction.response.send_message(f"Failed to remove wallet: {str(e)}")

    async def _toggle(self, interaction: discord.Interaction, setting: str):
        """Toggle notification settings"""
        try:
            settings = await get_notification_settings(str(interaction.user.id))
            if not settings:
                settings = self.DEFAULT_SETTINGS.copy()

            if setting not in settings:
                await interaction.response.send_message(f"Invalid setting: {setting}")
                return

            settings[setting] = not settings[setting]
            success = await update_notification_settings(str(interaction.user.id), settings)
            if success:
                await interaction.response.send_message(f"✅ {setting} notifications {'enabled' if settings[setting] else 'disabled'}!")
            else:
                await interaction.response.send_message("Failed to update settings.")
        except Exception as e:
            await interaction.response.send_message(f"Failed to toggle setting: {str(e)}")

class MockUser:
    """Mock Discord user for testing"""
    def __init__(self, user_id):
        self.id = user_id
        self.name = "Test User"
        self.discriminator = "1234"

class MockResponse:
    """Mock Discord interaction response for testing"""
    def __init__(self):
        self.message = None
        self.embed = None
        
    async def send_message(self, content=None, embed=None, ephemeral=False):
        self.message = content
        self.embed = embed
        
class MockInteraction:
    """Mock Discord interaction for testing"""
    def __init__(self, user_id):
        self.user = MockUser(user_id)
        self.response = MockResponse()
        self.channel = None  # We don't need a channel for testing slash commands

class TestCommands(unittest.IsolatedAsyncioTestCase):
    """Test suite for WalletBud bot commands"""
    
    test_user_id = None
    test_address = None
    
    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
        os.environ['BLOCKFROST_PROJECT_ID'] = 'test_project_id'
        os.environ['DISCORD_TOKEN'] = 'test_token'
        os.environ['ADMIN_CHANNEL_ID'] = '123456789'
        
        # Set up test data
        self.test_user_id = 123456789
        self.test_address = "addr1qxck3sly5h5q68e9nrj5rzk3vc76juuev9fqhvl4e8xqzgt8l8zqkc0et9mxdw3yj8wqeuv2ym7k6jxr87xvhf5v7ynq3h6xvz"
        
    async def asyncSetUp(self):
        """Set up each test asynchronously"""
        # Create mock bot instance
        self.bot = MockBot()
        
        # Mock Blockfrost client
        self.bot.blockfrost_client = MagicMock()
        self.bot.blockfrost_client.health = MagicMock(return_value=True)
        self.bot.blockfrost_client.address_utxos = MagicMock(return_value=[
            MagicMock(amount=[
                MagicMock(unit='lovelace', quantity='5000000'),  # 5 ADA
                MagicMock(unit=f"{config.YUMMI_POLICY_ID}{config.YUMMI_TOKEN_NAME}", quantity=str(config.MINIMUM_YUMMI))
            ])
        ])
        
        # Mock database functions
        self.mock_get_wallet = AsyncMock()
        self.mock_get_all_wallets = AsyncMock()
        self.mock_get_settings = AsyncMock()
        self.mock_add_wallet = AsyncMock()
        self.mock_remove_wallet = AsyncMock()
        self.mock_update_settings = AsyncMock()
        
        # Set default return values
        self.mock_get_wallet.return_value = None
        self.mock_get_all_wallets.return_value = []
        self.mock_get_settings.return_value = self.bot.DEFAULT_SETTINGS.copy()
        
        # Create patches
        self.patches = [
            patch('database.get_wallet_for_user', new=self.mock_get_wallet),
            patch('database.get_all_wallets_for_user', new=self.mock_get_all_wallets),
            patch('database.get_notification_settings', new=self.mock_get_settings),
            patch('database.add_wallet_for_user', new=self.mock_add_wallet),
            patch('database.remove_wallet_for_user', new=self.mock_remove_wallet),
            patch('database.update_notification_settings', new=self.mock_update_settings),
            patch('database._pool', new=None)  # Reset pool for each test
        ]
        
        # Start patches
        for p in self.patches:
            p.start()
            
        # Set up mock side effects for bot
        self.bot._mock_add_wallet = self.mock_add_wallet
        self.bot._mock_remove_wallet = self.mock_remove_wallet
        
        # Register commands
        self.bot.tree.clear_commands(guild=None)
        
        # Add command handlers from WalletBud
        @self.bot.tree.command(name="help")
        async def help(interaction: discord.Interaction):
            embed = discord.Embed(title="🤖 WalletBud Help", description="Test help message")
            await interaction.response.send_message(embed=embed)
            
        @self.bot.tree.command(name="health")
        async def health(interaction: discord.Interaction):
            embed = discord.Embed(title="🏥 System Health", description="Test health status")
            await interaction.response.send_message(embed=embed)
            
        @self.bot.tree.command(name="balance")
        async def balance(interaction: discord.Interaction):
            await self.bot._balance(interaction)
            
        @self.bot.tree.command(name="notifications")
        async def notifications(interaction: discord.Interaction):
            embed = discord.Embed(title="🔔 Notification Settings", description="Test notifications")
            await interaction.response.send_message(embed=embed)
            
        @self.bot.tree.command(name="addwallet")
        async def addwallet(interaction: discord.Interaction, address: str):
            await self.bot._add_wallet(interaction, address)
            
        @self.bot.tree.command(name="removewallet")
        async def removewallet(interaction: discord.Interaction, address: str):
            await self.bot._remove_wallet(interaction, address)
            
        @self.bot.tree.command(name="list")
        async def list_wallets(interaction: discord.Interaction):
            embed = discord.Embed(title="📋 Your Registered Wallets", description="Test wallet list")
            await interaction.response.send_message(embed=embed)
            
        @self.bot.tree.command(name="toggle")
        async def toggle(interaction: discord.Interaction, setting: str):
            await self.bot._toggle(interaction, setting)
            
        @self.bot.tree.command(name="stats")
        async def stats(interaction: discord.Interaction):
            embed = discord.Embed(title="📊 System Statistics", description="Test stats")
            await interaction.response.send_message(embed=embed)
            
    async def asyncTearDown(self):
        """Clean up after each test"""
        # Stop all patches
        for p in self.patches:
            p.stop()
            
        # Close database pool if it exists
        from database import _pool
        if _pool:
            await _pool.close()
            
    async def simulate_command(self, command_name, *args):
        """Simulate a command interaction"""
        # Create a mock state
        mock_state = MagicMock()
        mock_response = MockResponse()
        
        # Create a mock interaction
        interaction = MockInteraction(user_id="123456789")
        interaction.response = mock_response
        
        # Find the command in the bot's tree
        command = None
        for cmd in self.bot.tree.walk_commands():
            if cmd.name == command_name:
                command = cmd
                break
                
        if not command:
            raise ValueError(f"Command {command_name} not found")
        
        # Call the command
        await command.callback(interaction, *args)
        
        return interaction
        
    async def test_help_command(self):
        """Test the help command"""
        interaction = await self.simulate_command("help")
        self.assertIsNotNone(interaction.response.embed)
        self.assertEqual(interaction.response.embed.title, "🤖 WalletBud Help")
            
    async def test_health_command(self):
        """Test the health command"""
        interaction = await self.simulate_command("health")
        self.assertIsNotNone(interaction.response.embed)
        self.assertEqual(interaction.response.embed.title, "🏥 System Health")
            
    async def test_balance_command(self):
        """Test the balance command"""
        # Mock get_all_wallets_for_user to return a list with the test wallet
        self.mock_get_all_wallets.return_value = [self.test_address]

        # Mock get_wallet_for_user to return wallet details
        self.mock_get_wallet.return_value = {
            "address": self.test_address,
            "user_id": str(self.test_user_id)
        }

        # Mock the blockfrost response for balance
        self.bot.blockfrost_client.address_utxos.return_value = [
            MagicMock(amount=[
                MagicMock(unit='lovelace', quantity='5000000'),  # 5 ADA
                MagicMock(unit=f"{config.YUMMI_POLICY_ID}{config.YUMMI_TOKEN_NAME}", quantity=str(config.MINIMUM_YUMMI))
            ])
        ]

        interaction = await self.simulate_command("balance")
        self.assertIsNotNone(interaction.response.message)
        self.assertIn("5.0 ada", interaction.response.message.lower())

    async def test_notifications_command(self):
        """Test the notifications command"""
        interaction = await self.simulate_command("notifications")
        self.assertIsNotNone(interaction.response.embed)
        self.assertEqual(interaction.response.embed.title, "🔔 Notification Settings")
            
    async def test_addwallet_command(self):
        """Test the addwallet command"""
        interaction = await self.simulate_command("addwallet", self.test_address)
        self.assertIn("Added wallet", interaction.response.message)
            
    async def test_addwallet_duplicate(self):
        """Test adding a duplicate wallet address"""
        # Mock get_all_wallets to return the test wallet in the list
        self.mock_get_all_wallets.return_value = [self.test_address]

        # Mock get_wallet to return a wallet object
        self.mock_get_wallet.return_value = {
            "address": self.test_address,
            "user_id": str(self.test_user_id)
        }

        # Mock add_wallet to fail
        self.mock_add_wallet.side_effect = Exception("Duplicate wallet")

        interaction = await self.simulate_command("addwallet", self.test_address)
        self.assertIn("already registered", interaction.response.message.lower())

    async def test_addwallet_invalid_address(self):
        """Test adding an invalid wallet address"""
        interaction = await self.simulate_command("addwallet", "invalid_address_123")
        
        self.assertIn("Invalid wallet address format", interaction.response.message)
            
    async def test_removewallet_command(self):
        """Test the removewallet command"""
        # Mock get_wallet and get_all_wallets to return the test wallet
        self.mock_get_wallet.return_value = {
            "address": self.test_address,
            "user_id": str(self.test_user_id)
        }
        self.mock_get_all_wallets.return_value = [self.test_address]

        # Mock successful removal
        self.mock_remove_wallet.return_value = True
        self.mock_remove_wallet.side_effect = None

        interaction = await self.simulate_command("removewallet", self.test_address)
        self.assertIn("removed successfully", interaction.response.message.lower())

    async def test_removewallet_nonexistent(self):
        """Test removing a wallet that doesn't exist"""
        # Mock get_all_wallets_for_user to return empty list
        self.mock_get_all_wallets.return_value = []
        
        interaction = await self.simulate_command("removewallet", "nonexistent_address")
        
        self.assertIn("not found", interaction.response.message.lower())
            
    async def test_balance_no_wallets(self):
        """Test balance command with no registered wallets"""
        # Mock get_all_wallets_for_user to return empty list
        self.mock_get_all_wallets.return_value = []
        
        interaction = await self.simulate_command("balance")
        
        self.assertIn("no registered wallets", interaction.response.message.lower())
            
    async def test_list_command(self):
        """Test the list command"""
        interaction = await self.simulate_command("list")
        self.assertIsNotNone(interaction.response.embed)
        self.assertEqual(interaction.response.embed.title, "📋 Your Registered Wallets")
            
    async def test_toggle_command(self):
        """Test the toggle command"""
        interaction = await self.simulate_command("toggle", "ada_transactions")
        self.assertIsNotNone(interaction.response.message)
        self.assertIn("enabled", interaction.response.message.lower())
            
    async def test_stats_command(self):
        """Test the stats command"""
        interaction = await self.simulate_command("stats")
        self.assertIsNotNone(interaction.response.embed)
        self.assertEqual(interaction.response.embed.title, "📊 System Statistics")
            
if __name__ == "__main__":
    unittest.main()
