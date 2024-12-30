import os
import asyncio
import logging
import blockfrost
from dotenv import load_dotenv
from config import (
    BLOCKFROST_API_KEY,
    MAX_REQUESTS_PER_SECOND,
    BURST_LIMIT,
    RATE_LIMIT_DELAY
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_blockfrost_connection():
    """Test Blockfrost API connection and basic functionality"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get API key
        if not BLOCKFROST_API_KEY:
            logger.error("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")
            return False
            
        logger.info(f"API Key prefix: {BLOCKFROST_API_KEY[:10]}...")
        logger.info(f"Rate limits: {MAX_REQUESTS_PER_SECOND} req/s, burst: {BURST_LIMIT}")
            
        # Initialize client
        client = blockfrost.BlockFrostApi(
            project_id=BLOCKFROST_API_KEY,
            base_url="https://cardano-mainnet.blockfrost.io/api/v0"  # Correct mainnet URL
        )
        
        logger.info("Testing API connection...")
        
        # Test 1: Get specific address
        try:
            logger.info("Testing address endpoint...")
            test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
            address = client.address(test_address)
            logger.info(f"Address info: {address}")
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.error(f"Address error: {str(e)}")
            return False
            
        # Test 2: Get address UTXOs and transactions
        try:
            logger.info("Testing address UTXOs and transactions...")
            utxos = client.address_utxos(test_address)
            logger.info(f"Address UTXOs: {utxos}")
            
            txs = client.address_transactions(test_address)
            logger.info(f"Address transactions: {txs}")
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.error(f"Address UTXOs/transactions error: {str(e)}")
            return False
            
        # Test 3: Test rate limiting
        try:
            logger.info("Testing rate limiting...")
            for i in range(12):  # Should trigger rate limit
                client.address(test_address)
                logger.info(f"Request {i+1} successful")
                await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            if "rate limit" in str(e).lower():
                logger.info("Rate limit test passed - limit detected correctly")
            else:
                logger.error(f"Rate limit test error: {str(e)}")
                return False
        
        logger.info("All tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

async def main():
    success = await test_blockfrost_connection()
    if success:
        logger.info("All Blockfrost tests passed!")
    else:
        logger.error("Some Blockfrost tests failed!")

if __name__ == "__main__":
    asyncio.run(main())
