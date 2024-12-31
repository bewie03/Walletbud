import unittest
import asyncio
import aiohttp
import json
import hmac
import hashlib
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bot import WalletBud
from aiohttp import web

# Load environment variables
load_dotenv()

# Get webhook configuration from environment
WEBHOOK_URL = "http://localhost:49152/webhooks/blockfrost"
TX_WEBHOOK_ID = "c7663027-fcbe-465a-a43d-753bd6543796"  # Updated transaction webhook ID
DEL_WEBHOOK_ID = "31843138-0bd9-4fb5-a735-25978a064f4d"  # Updated delegation webhook ID

# Test transaction webhook payload
TRANSACTION_PAYLOAD = {
    "event_type": "transaction",  # Must match config.WEBHOOKS key
    "tx_hash": "735262c68b5fa220dee2b447d0d1dd44e0800ba6212dcea7955c561f365fb0e9",
    "block": "3423",
    "slot": "12323",
    "index": 1,
    "confirmations": 2,
    "addresses": [
        "addr1qy8ac7qqy0vtulyl7wntmsxc6wex80gvcyjy33qffrhm7sh927ysx5sftuw0dlft05dz3c7revpf527z2zf2p2czd2ps8k9liw"
    ]
}

# Test delegation webhook payload
DELEGATION_PAYLOAD = {
    "event_type": "delegation",  # Must match config.WEBHOOKS key
    "tx_hash": "735262c68b5fa220dee2b447d0d1dd44e0800ba6212dcea7955c561f365fb0e9",
    "block": "3423",
    "slot": "12323",
    "index": 1,
    "confirmations": 1,
    "stake_address": "stake1uxpdrerp9wrxunfh6ukyv5267j70fzxgw0fr3z8zeac5vyqhf9jhy",
    "pool_id": "pool1z5uqdk7dzdxaae5633fqfcu2eqzy3a3rgtuvy087fdld7yws0xt",
    "action": "registered"
}

class TestWebhooks(unittest.IsolatedAsyncioTestCase):
    """Test suite for WalletBud webhooks"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        # Create a mock token if not present
        if not os.getenv('DISCORD_TOKEN'):
            os.environ['DISCORD_TOKEN'] = 'mock_token'
            
    async def asyncSetUp(self):
        """Set up each test"""
        # Start the bot server
        self.bot = WalletBud()
        
        # Initialize the bot without connecting to Discord
        await self.bot._async_setup_hook()  # Set up the bot without actually connecting to Discord
        await self.bot.init_blockfrost()  # Initialize Blockfrost client
        
        # Start the webhook server
        await self.bot.runner.setup()
        site = web.TCPSite(self.bot.runner, '0.0.0.0', self.bot.port)
        await site.start()
        await asyncio.sleep(1)  # Give server time to start
        
        # Create session
        self.session = aiohttp.ClientSession()
        
    async def asyncTearDown(self):
        """Clean up after each test"""
        # Close session
        if hasattr(self, 'session'):
            await self.session.close()
            
        # Clean up bot
        if hasattr(self, 'bot'):
            await self.bot.runner.cleanup()
            await self.bot.close()
            
    async def send_test_webhook(self, payload, webhook_id, signature=None):
        """Send a test webhook request"""
        url = f"http://localhost:49152/webhooks/blockfrost"
        
        # Calculate signature if not provided
        if signature is None:
            message = json.dumps(payload)
            signature = hmac.new(
                os.getenv('WEBHOOK_SECRET', '118dcfef-dfe3-43c1-ac37-017e0bf561fd').encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()
            
        headers = {
            'Webhook-Id': webhook_id,
            'Auth-Token': os.getenv('WEBHOOK_SECRET', '118dcfef-dfe3-43c1-ac37-017e0bf561fd'),
            'Signature': signature
        }
        
        async with self.session.post(url, json=payload, headers=headers) as response:
            return response.status, await response.text()
            
    async def test_transaction_webhook(self):
        """Test transaction webhook with valid payload"""
        status, _ = await self.send_test_webhook(TRANSACTION_PAYLOAD, TX_WEBHOOK_ID)
        self.assertEqual(status, 200, "Transaction webhook should return 200 OK")
        
    async def test_delegation_webhook(self):
        """Test delegation webhook with valid payload"""
        status, _ = await self.send_test_webhook(DELEGATION_PAYLOAD, DEL_WEBHOOK_ID)
        self.assertEqual(status, 200, "Delegation webhook should return 200 OK")
        
    async def test_invalid_webhook_id(self):
        """Test webhook with invalid ID"""
        status, _ = await self.send_test_webhook(TRANSACTION_PAYLOAD, "invalid_id")
        self.assertEqual(status, 401, "Invalid webhook ID should return 401 Unauthorized")
        
    async def test_invalid_signature(self):
        """Test webhook with invalid signature"""
        status, _ = await self.send_test_webhook(
            TRANSACTION_PAYLOAD, 
            TX_WEBHOOK_ID,
            signature="invalid_signature"
        )
        self.assertEqual(status, 401, "Invalid signature should return 401 Unauthorized")
        
    async def test_missing_event_type(self):
        """Test webhook with missing event_type"""
        invalid_payload = TRANSACTION_PAYLOAD.copy()
        del invalid_payload["event_type"]
        status, _ = await self.send_test_webhook(invalid_payload, TX_WEBHOOK_ID)
        self.assertEqual(status, 400, "Missing event_type should return 400 Bad Request")
        
    async def test_invalid_event_type(self):
        """Test webhook with invalid event_type"""
        invalid_payload = TRANSACTION_PAYLOAD.copy()
        invalid_payload["event_type"] = "invalid_type"
        status, _ = await self.send_test_webhook(invalid_payload, TX_WEBHOOK_ID)
        self.assertEqual(status, 400, "Invalid event_type should return 400 Bad Request")
        
if __name__ == "__main__":
    unittest.main()
