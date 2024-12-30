import os
from dotenv import load_dotenv
from blockfrost import BlockFrostApi

def test_blockfrost_connection():
    """Test Blockfrost API connection"""
    # Load environment variables
    load_dotenv()
    
    # Get API key
    api_key = os.getenv('BLOCKFROST_API_KEY')
    if not api_key:
        print("ERROR: BLOCKFROST_API_KEY not set")
        return False
        
    try:
        # Create client
        client = BlockFrostApi(
            project_id=api_key
        )
        
        # Test address endpoint
        test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
        address_info = client.address(test_address)
        print(f"Address info: {address_info}")
        
        print("Blockfrost connection test passed!")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to test Blockfrost connection: {str(e)}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"Response details: {e.response.text}")
        return False

if __name__ == "__main__":
    test_blockfrost_connection()
