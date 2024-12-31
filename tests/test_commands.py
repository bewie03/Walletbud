import unittest
import asyncio
import discord
from discord.ext import commands
from bot import WalletBud
import config
import os

class TestCommands(unittest.IsolatedAsyncioTestCase):
    """Test suite for WalletBud bot commands"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        
    async def asyncSetUp(self):
        """Set up each test asynchronously"""
        # Create bot instance
        self.bot = WalletBud()
        
        # Sync commands
        await self.bot._sync_commands()
        
        # Set up test data
        self.test_user_id = 123456789
        self.test_address = "addr1qy2jt0qpqz2z9zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y8zx5y"
        
    class MockInteraction:
        """Mock Discord Interaction for testing"""
        def __init__(self, data, state):
            self.data = data
            self.state = state
            self.user = None
            self._response = None
            
        @property
        def response(self):
            return self._response
            
        @response.setter
        def response(self, value):
            self._response = value
            
        def _get_client(self):
            return self.state._get_client()

    async def simulate_command(self, command_name, *args):
        """Simulate a command interaction"""
        # Create a mock state
        mock_state = type('MockState', (), {
            '_get_client': lambda self: self.bot,
            'http': self.bot.http,
            'client': self.bot
        })()
        mock_state.bot = self.bot  # Add bot reference for the lambda
        
        # Create mock response methods
        async def mock_send_message(*args, **kwargs):
            pass
        async def mock_defer(*args, **kwargs):
            pass
            
        # Create mock response
        mock_response = type('MockResponse', (), {
            'send_message': mock_send_message,
            'defer': mock_defer,
            'is_done': lambda: False
        })()
        
        # Create a mock interaction
        interaction = self.MockInteraction(
            data={
                "id": "123456789",
                "application_id": "123456789",
                "type": 2,  # APPLICATION_COMMAND
                "token": "mock_token",
                "version": 1,
                "data": {
                    "id": "123456789",
                    "name": command_name,
                    "type": 1,  # CHAT_INPUT
                }
            },
            state=mock_state
        )
        
        # Add user to interaction
        interaction.user = discord.Object(id=self.test_user_id)
        
        # Set response
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
        # Help command should work without errors
        self.assertTrue(True)
            
    async def test_health_command(self):
        """Test the health command"""
        interaction = await self.simulate_command("health")
        # Health command should work without errors
        self.assertTrue(True)
            
    async def test_balance_command(self):
        """Test the balance command"""
        interaction = await self.simulate_command("balance")
        # Balance command should work without errors
        self.assertTrue(True)
            
    async def test_notifications_command(self):
        """Test the notifications command"""
        interaction = await self.simulate_command("notifications")
        # Notifications command should work without errors
        self.assertTrue(True)
            
    async def test_addwallet_command(self):
        """Test the addwallet command"""
        interaction = await self.simulate_command("addwallet", self.test_address)
        # Addwallet command should work without errors
        self.assertTrue(True)
            
    async def test_removewallet_command(self):
        """Test the removewallet command"""
        interaction = await self.simulate_command("removewallet", self.test_address)
        # Removewallet command should work without errors
        self.assertTrue(True)
            
    async def test_list_command(self):
        """Test the list command"""
        interaction = await self.simulate_command("list_wallets")
        # List command should work without errors
        self.assertTrue(True)
            
if __name__ == "__main__":
    unittest.main()
