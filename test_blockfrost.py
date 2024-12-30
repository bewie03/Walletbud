import os
import logging
import blockfrost
from dotenv import load_dotenv
from config import BLOCKFROST_BASE_URL

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_blockfrost_connection():
    """Test Blockfrost API connection and basic functionality"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get API key
        project_id = os.getenv('BLOCKFROST_API_KEY')
        if not project_id:
            logger.error("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")
            return False
            
        logger.info(f"Using Blockfrost URL: {BLOCKFROST_BASE_URL}")
        logger.info(f"API Key prefix: {project_id[:10]}...")
            
        # Initialize client
        client = blockfrost.BlockFrostApi(
            project_id=project_id
        )
        
        logger.info("Testing API connection...")
        
        # Test 1: Get API info
        try:
            logger.info("Testing API info...")
            info = client.info()
            logger.info(f"API Info: {info}")
        except Exception as e:
            logger.error(f"API info error: {str(e)}")
        
        # Test 2: Get specific address
        try:
            logger.info("Testing address endpoint...")
            test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
            address = client.address(test_address)
            logger.info(f"Address info: {address}")
        except Exception as e:
            logger.error(f"Address error: {str(e)}")
        
        logger.info("Tests completed")
        return True
        
    except blockfrost.ApiError as e:
        if e.status_code == 403:
            logger.error("Invalid API key")
        elif e.status_code == 429:
            logger.error("Rate limit exceeded")
        else:
            logger.error(f"API Error: {str(e)}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    test_blockfrost_connection()
