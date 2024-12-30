import os
from dotenv import load_dotenv

load_dotenv()

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
if not DISCORD_TOKEN:
    raise ValueError("No Discord token found! Make sure DISCORD_TOKEN is set in .env")

COMMAND_PREFIX = '!'  

# Blockfrost Configuration
BLOCKFROST_API_KEY = os.getenv('BLOCKFROST_API_KEY')
if not BLOCKFROST_API_KEY:
    raise ValueError("No Blockfrost API key found! Make sure BLOCKFROST_API_KEY is set in .env")

BLOCKFROST_BASE_URL = 'https://cardano-mainnet.blockfrost.io/api/v0'

# Database Configuration
DATABASE_NAME = 'wallets.db'  

# Token Configuration
REQUIRED_BUD_TOKENS = int(os.getenv('REQUIRED_BUD_TOKENS', '20000'))
YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID', '078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924')

# Transaction monitoring settings
TRANSACTION_CHECK_INTERVAL = int(os.getenv('TRANSACTION_CHECK_INTERVAL', '5'))  
YUMMI_CHECK_INTERVAL = int(os.getenv('YUMMI_CHECK_INTERVAL', '6'))  
MAX_TX_HISTORY = int(os.getenv('MAX_TX_HISTORY', '10'))  

# Polling Configuration
POLLING_INTERVAL = 900  
