# WalletBud Discord Bot

A Discord bot for monitoring Cardano wallets and YUMMI token transactions.

## Features

- Monitor Cardano wallets for transactions
- Track YUMMI token balances and DApp interactions
- Receive DM notifications for:
  - ADA transactions
  - Token changes
  - NFT updates
  - Staking rewards
  - Stake key changes
  - Low balance alerts
  - Policy expiry alerts
  - Delegation status
  - DApp interactions

## Commands

- `/addwallet` - Add a Cardano wallet (DM only, requires 25,000 YUMMI tokens)
- `/removewallet` - Remove a wallet from tracking
- `/listwallets` - View your registered wallets
- `/balance` - Check your wallet balances
- `/notifications` - View notification settings
- `/toggle` - Toggle specific notifications
- `/health` - Check bot's health status
- `/help` - Show available commands

## Requirements

- Python 3.11.7
- PostgreSQL 13+
- Discord Bot Token
- Blockfrost API Key

## Environment Variables

Create a `.env` file with the following variables (see `.env.example` for details):
```env
# Discord Bot Configuration
DISCORD_TOKEN=your_discord_token_here
COMMAND_PREFIX=!

# Blockfrost API Configuration
BLOCKFROST_API_KEY=your_blockfrost_api_key_here
BLOCKFROST_PROJECT_ID=your_project_id_here

# Database Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/walletbud

# Admin Configuration
ADMIN_CHANNEL_ID=your_admin_channel_id_here
LOG_LEVEL=INFO

# YUMMI Token Configuration
YUMMI_POLICY_ID=078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924
YUMMI_ASSET_NAME=59554d4d49
```

## Features

### Wallet Monitoring
- Real-time transaction monitoring
- ADA balance tracking
- Token and NFT tracking
- DApp interaction detection
- Staking rewards monitoring
- Delegation status tracking

### Notifications
- Customizable notification settings
- All notifications enabled by default
- DM-based notifications for privacy
- Rate-limited to prevent spam

### Security
- DM-only sensitive commands
- YUMMI token requirement for registration
- Rate limiting for API requests
- Exponential backoff for retries

### Error Handling
- Admin channel notifications for errors
- Automatic retry for failed API requests
- Transaction-safe database operations
- Comprehensive error logging

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up PostgreSQL database
4. Configure environment variables
5. Run the bot: `python bot.py`

## Contributing

Feel free to open issues or submit pull requests for any improvements.

## License

This project is licensed under the MIT License - see the LICENSE file for details.