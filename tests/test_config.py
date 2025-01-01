import pytest
import os
from unittest.mock import patch, MagicMock
import config
from config import (
    validate_blockfrost_id,
    validate_webhook_secret,
    validate_ip_ranges,
    validate_base_url,
    validate_discord_token,
    validate_database_url,
    load_env_file
)

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {
        'BLOCKFROST_PROJECT_ID': 'mainnetabcd1234',
        'DISCORD_TOKEN': 'valid.discord.token',
        'DATABASE_URL': 'postgresql://user:pass@localhost:5432/db',
        'BLOCKFROST_IP_RANGES': '["192.168.1.0/24"]',
        'BLOCKFROST_WEBHOOK_SECRET': 'a' * 32,
        'BLOCKFROST_BASE_URL': 'https://cardano-mainnet.blockfrost.io/api/v0'
    }):
        yield

def test_validate_blockfrost_id():
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
        
        # Valid ID
        assert validate_blockfrost_id('mainnetabcd1234', 'test') == 'mainnetabcd1234'
        
        # Invalid format
        with pytest.raises(ValueError):
            validate_blockfrost_id('invalid', 'test')
            
        # Empty value
        with pytest.raises(ValueError):
            validate_blockfrost_id('', 'test')

def test_validate_webhook_secret():
    # Valid secret
    valid_secret = 'a' * 32
    assert validate_webhook_secret(valid_secret, 'test') == valid_secret
    
    # Too short
    with pytest.raises(ValueError):
        validate_webhook_secret('short', 'test')
        
    # Invalid characters
    with pytest.raises(ValueError):
        validate_webhook_secret('!' * 32, 'test')

def test_validate_ip_ranges():
    # Valid CIDR
    valid_ranges = '["192.168.1.0/24", "10.0.0.0/8"]'
    result = validate_ip_ranges(valid_ranges, 'test')
    assert len(result) == 2
    assert "192.168.1.0/24" in result
    
    # Invalid JSON
    with pytest.raises(ValueError):
        validate_ip_ranges('invalid json', 'test')
        
    # Invalid CIDR
    with pytest.raises(ValueError):
        validate_ip_ranges('["256.256.256.256/33"]', 'test')

def test_validate_base_url():
    # Valid URL
    valid_url = 'https://cardano-mainnet.blockfrost.io/api/v0'
    assert validate_base_url(valid_url, 'test') == valid_url
    
    # Invalid URL
    with pytest.raises(ValueError):
        validate_base_url('https://invalid.url', 'test')
        
    # Empty URL
    with pytest.raises(ValueError):
        validate_base_url('', 'test')

def test_validate_discord_token():
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
        
        # Valid token
        valid_token = 'a' * 59
        assert validate_discord_token(valid_token, 'test') == valid_token
        
        # Invalid format
        with pytest.raises(ValueError):
            validate_discord_token('invalid', 'test')
            
        # Empty token
        with pytest.raises(ValueError):
            validate_discord_token('', 'test')

def test_validate_database_url():
    with patch('asyncpg.connect') as mock_connect:
        mock_connect.return_value = MagicMock()
        
        # Valid URL
        valid_url = 'postgresql://user:pass@localhost:5432/db'
        assert validate_database_url(valid_url, 'test') == valid_url
        
        # Invalid scheme
        with pytest.raises(ValueError):
            validate_database_url('mysql://invalid', 'test')
            
        # Missing components
        with pytest.raises(ValueError):
            validate_database_url('postgresql://incomplete', 'test')

def test_load_env_file(mock_env):
    # Test with existing file
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        config = load_env_file()
        assert isinstance(config, dict)
        
    # Test with missing file
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = False
        config = load_env_file()
        assert isinstance(config, dict)
        
    # Test with invalid file
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        with patch('dotenv.load_dotenv', side_effect=Exception):
            config = load_env_file()
            assert isinstance(config, dict)
