import os
import logging
import requests
from dotenv import load_dotenv

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
            
        # Determine network from API key
        network = "mainnet"
        if project_id.startswith("preprod"):
            network = "preprod"
        elif project_id.startswith("preview"):
            network = "preview"
            
        # Get the correct base URL
        base_url = {
            'mainnet': 'https://cardano-mainnet.blockfrost.io/api/v0',
            'preprod': 'https://cardano-preprod.blockfrost.io/api/v0',
            'preview': 'https://cardano-preview.blockfrost.io/api/v0'
        }[network]
            
        logger.info(f"Using Blockfrost {network} network")
        logger.info(f"API Key prefix: {project_id[:8]}...")
            
        # Set up headers
        headers = {
            'project_id': project_id
        }
        
        logger.info("Testing API connection...")
        
        # Test 1: Get health
        try:
            logger.info("Testing health endpoint...")
            response = requests.get(f"{base_url}/health", headers=headers)
            if response.status_code == 200:
                logger.info(f"Health check successful: {response.json()}")
            else:
                logger.error(f"Health check failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Health check error: {str(e)}")
        
        # Test 2: Get network info
        try:
            logger.info("Testing network endpoint...")
            response = requests.get(f"{base_url}/network", headers=headers)
            if response.status_code == 200:
                logger.info(f"Network info: {response.json()}")
            else:
                logger.error(f"Network check failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Network check error: {str(e)}")
        
        # Test 3: Get specific address
        try:
            logger.info("Testing address endpoint...")
            test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
            response = requests.get(f"{base_url}/addresses/{test_address}", headers=headers)
            if response.status_code == 200:
                logger.info(f"Address info: {response.json()}")
            else:
                logger.error(f"Address check failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Address check error: {str(e)}")
            
        logger.info("Tests completed")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_blockfrost_connection()
