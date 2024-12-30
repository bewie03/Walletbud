import os
from dotenv import load_dotenv

load_dotenv()

# Discord Bot Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
COMMAND_PREFIX = '/'

# Blockfrost Configuration
BLOCKFROST_API_KEY = os.getenv('BLOCKFROST_API_KEY')
BLOCKFROST_BASE_URL = 'https://cardano-mainnet.blockfrost.io/api/v0'

# Database Configuration
DATABASE_NAME = 'walletbud.db'

# Token Configuration
REQUIRED_BUD_TOKENS = 20000
YUMMI_POLICY_ID = os.getenv('YUMMI_POLICY_ID', '078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924')

# Wallet monitoring configuration
MIN_YUMMI_REQUIRED = 20000  # Minimum YUMMI tokens required to monitor wallet
TRANSACTION_CHECK_INTERVAL = 5  # Minutes between transaction checks
MAX_TX_HISTORY = 10  # Number of recent transactions to check

# Polling Configuration
POLLING_INTERVAL = 900  # 15 minutes in seconds
