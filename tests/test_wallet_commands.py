import pytest
import discord
from unittest.mock import Mock, patch, AsyncMock
from cogs.wallet_commands import WalletCommands
from config import MINIMUM_YUMMI

@pytest.fixture
def bot():
    bot = Mock()
    bot.blockfrost = Mock()
    bot.rate_limited_request = AsyncMock()
    return bot

@pytest.fixture
def wallet_commands(bot):
    return WalletCommands(bot)

@pytest.mark.asyncio
async def test_validate_address():
    """Test address validation"""
    cog = WalletCommands(Mock())
    
    # Test valid address
    valid_addr = "addr1qxck7q6s8dj0yqzc4phed4qy0pu0rgxq3tdfwrfwty4fe9sjxexm2f8rh8ml4xawvpxx4xqj9ghyc7p2c9ruyjx3tqmqkxvhdn"
    assert await cog.validate_address(valid_addr) == True
    
    # Test invalid address
    invalid_addr = "invalid_address"
    assert await cog.validate_address(invalid_addr) == False

@pytest.mark.asyncio
async def test_get_address_balance(wallet_commands, bot):
    """Test balance fetching with caching"""
    mock_summary = Mock()
    mock_summary.amount = [
        Mock(unit='lovelace', quantity='1000000'),
        Mock(unit='token1', quantity='100')
    ]
    bot.rate_limited_request.return_value = mock_summary
    
    # First call should hit API
    balance = await wallet_commands.get_address_balance("test_address")
    assert balance['lovelace'] == 1000000
    assert balance['tokens']['token1'] == '100'
    assert bot.rate_limited_request.called
    
    # Second call should hit cache
    bot.rate_limited_request.reset_mock()
    balance = await wallet_commands.get_address_balance("test_address")
    assert balance['lovelace'] == 1000000
    assert not bot.rate_limited_request.called

@pytest.mark.asyncio
async def test_add_command(wallet_commands):
    """Test add wallet command"""
    interaction = AsyncMock()
    interaction.user.id = "123"
    
    # Mock address validation
    with patch.object(wallet_commands, 'validate_address', return_value=True):
        # Mock blockfrost check
        wallet_commands.bot.rate_limited_request.return_value = Mock(stake_address="stake_test")
        
        # Test successful add
        with patch('cogs.wallet_commands.add_wallet_for_user', new_callable=AsyncMock) as mock_add:
            mock_add.return_value = True
            await wallet_commands.add(interaction, "test_address")
            assert interaction.followup.send.called
            assert "Successfully added wallet" in interaction.followup.send.call_args[0][0]

@pytest.mark.asyncio
async def test_balance_command(wallet_commands):
    """Test balance command"""
    interaction = AsyncMock()
    interaction.user.id = "123"
    
    # Mock getting wallets
    with patch('cogs.wallet_commands.get_all_wallets_for_user', new_callable=AsyncMock) as mock_get:
        mock_get.return_value = ["addr1", "addr2"]
        
        # Mock balance fetching
        mock_balance = {'lovelace': 1000000, 'tokens': {'token1': '100'}}
        with patch.object(wallet_commands, 'get_address_balance', return_value=mock_balance, new_callable=AsyncMock):
            await wallet_commands.balance(interaction)
            
            # Verify embed was sent
            assert interaction.followup.send.called
            embed = interaction.followup.send.call_args[1]['embed']
            assert isinstance(embed, discord.Embed)
            assert "Your Wallet Balances" in embed.title
            assert "Total ADA: 2.00" in embed.footer.text  # 2 ADA total from 2 addresses

@pytest.mark.asyncio
async def test_error_handling(wallet_commands):
    """Test error handling in commands"""
    interaction = AsyncMock()
    
    # Test API error
    wallet_commands.bot.rate_limited_request.side_effect = Exception("API Error")
    await wallet_commands.add(interaction, "test_address")
    assert interaction.followup.send.called
    assert "error occurred" in interaction.followup.send.call_args[0][0].lower()

@pytest.mark.asyncio
async def test_yummi_balance_check(wallet_commands):
    """Test YUMMI balance verification"""
    interaction = AsyncMock()
    
    # Mock insufficient YUMMI balance
    with patch('cogs.wallet_commands.check_yummi_balance') as mock_check:
        mock_check.side_effect = Exception(f"Insufficient YUMMI balance. Required: {MINIMUM_YUMMI}")
        await wallet_commands.add(interaction, "test_address")
        assert interaction.followup.send.called
        assert "YUMMI" in interaction.followup.send.call_args[0][0]
