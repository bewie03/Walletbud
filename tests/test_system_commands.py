import pytest
import discord
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timedelta
from cogs.system_commands import SystemCommands

@pytest.fixture
def bot():
    bot = Mock()
    bot.blockfrost = Mock()
    bot.pool = Mock()
    bot.rate_limited_request = AsyncMock()
    bot.start_time = datetime.now() - timedelta(days=1)
    return bot

@pytest.fixture
def system_commands(bot):
    return SystemCommands(bot)

@pytest.mark.asyncio
async def test_help_command(system_commands):
    """Test dynamic help command"""
    interaction = AsyncMock()
    
    # Mock bot commands
    mock_commands = [
        Mock(name='test1', description='Test 1', extras={'category': 'Test'}),
        Mock(name='test2', description='Test 2', extras={'category': 'Test'})
    ]
    system_commands.bot.tree.walk_commands = Mock(return_value=mock_commands)
    
    await system_commands.help(interaction)
    assert interaction.response.send_message.called
    embed = interaction.response.send_message.call_args[1]['embed']
    assert isinstance(embed, discord.Embed)
    assert "WalletBud Help" in embed.title

@pytest.mark.asyncio
async def test_health_command(system_commands):
    """Test health command with metrics"""
    interaction = AsyncMock()
    
    # Mock health checks
    with patch.object(system_commands, '_check_health_metrics') as mock_health:
        mock_health.return_value = {
            'discord': {'connected': True, 'latency': 100},
            'database': {'connected': True, 'error': None},
            'blockfrost': {'healthy': True, 'error': None},
            'system': {
                'memory_used': 100.0,
                'memory_percent': 50.0,
                'cpu_percent': 30.0,
                'uptime': timedelta(days=1)
            }
        }
        
        await system_commands.health(interaction)
        assert interaction.followup.send.called
        embed = interaction.followup.send.call_args[1]['embed']
        assert isinstance(embed, discord.Embed)
        assert "System Health Check" in embed.title

@pytest.mark.asyncio
async def test_health_monitor(system_commands):
    """Test health monitoring background task"""
    with patch.object(system_commands, '_check_health_metrics') as mock_health:
        # Simulate healthy state
        mock_health.return_value = {
            'blockfrost': {'healthy': True},
            'system': {'memory_percent': 50, 'cpu_percent': 30}
        }
        await system_commands.health_monitor()
        assert system_commands._health_cache == mock_health.return_value
        
        # Simulate unhealthy state
        mock_health.return_value = {
            'blockfrost': {'healthy': False},
            'system': {'memory_percent': 95, 'cpu_percent': 90}
        }
        await system_commands.health_monitor()
        assert system_commands._health_cache == mock_health.return_value

@pytest.mark.asyncio
async def test_notifications_command(system_commands):
    """Test notifications command with pagination"""
    interaction = AsyncMock()
    
    # Mock settings
    with patch('cogs.system_commands.get_notification_settings', new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {
            'ada_transactions': True,
            'token_changes': False
        }
        
        await system_commands.notifications(interaction)
        assert interaction.response.send_message.called
        embed = interaction.response.send_message.call_args[1]['embed']
        assert isinstance(embed, discord.Embed)
        assert "Notification Settings" in embed.title

@pytest.mark.asyncio
async def test_toggle_command(system_commands):
    """Test toggle command with validation"""
    interaction = AsyncMock()
    
    # Test valid setting
    with patch('cogs.system_commands.update_notification_setting', new_callable=AsyncMock) as mock_update:
        mock_update.return_value = True
        await system_commands.toggle(interaction, 'ada_transactions', True)
        assert interaction.response.send_message.called
        assert "enabled" in interaction.response.send_message.call_args[0][0].lower()
    
    # Test invalid setting
    await system_commands.toggle(interaction, 'invalid_setting', True)
    assert "Invalid setting" in interaction.response.send_message.call_args[0][0]

@pytest.mark.asyncio
async def test_embed_pagination(system_commands):
    """Test embed pagination"""
    title = "Test"
    description = "Test Description"
    
    # Create fields that exceed Discord's limit
    fields = [
        {'name': f'Field {i}', 'value': 'x' * 1000, 'inline': False}
        for i in range(10)
    ]
    
    embeds = system_commands._create_paginated_embed(title, fields, description)
    assert len(embeds) > 1  # Should create multiple embeds
    assert all(isinstance(embed, discord.Embed) for embed in embeds)
    assert embeds[0].title == title
    assert embeds[1].title == f"{title} (cont.)"
