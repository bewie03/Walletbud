# WalletBud Discord Bot

A Discord bot for monitoring Cardano wallets with YUMMI token validation and comprehensive transaction tracking.

## Key Features

### Wallet Monitoring
- Real-time transaction tracking
- ADA balance monitoring
- Token and NFT change detection
- DApp interaction tracking (SundaeSwap, MinSwap, etc.)
- Staking rewards and delegation monitoring
- Policy expiry alerts

### Smart Notifications
- Customizable DM notifications
- Configurable alerts for:
  - ADA transactions
  - Token/NFT changes
  - Staking rewards
  - Delegation status
  - Low balance warnings
  - DApp interactions
- Rate-limited to prevent spam

### Security & Reliability
- DM-only sensitive commands
- YUMMI token validation (25,000 minimum)
- Rate-limited API requests
- Automatic retry with exponential backoff
- Admin channel error reporting

## Commands

- `/addwallet <address>` - Register a wallet (DM only, requires YUMMI tokens)
- `/removewallet <address>` - Unregister a wallet
- `/listwallets` - View your registered wallets
- `/balance` - Check wallet balances
- `/notifications` - View notification settings
- `/toggle <type>` - Toggle specific notifications
- `/health` - Check bot status
- `/help` - Show all commands

## Setup

### Requirements
- Python 3.11.7+
- PostgreSQL 13+
- Discord Bot Token
- Blockfrost API Key
- Heroku account (for deployment)

### Environment Variables
```env
# Discord Configuration
DISCORD_TOKEN=your_discord_bot_token
ADMIN_CHANNEL_ID=your_admin_channel_id
COMMAND_PREFIX=!

# Blockfrost API Configuration
BLOCKFROST_PROJECT_ID=your_blockfrost_project_id
BLOCKFROST_BASE_URL=https://cardano-mainnet.blockfrost.io/api/v0

# YUMMI Token Configuration
YUMMI_POLICY_ID=078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924
YUMMI_TOKEN_NAME=59554d4d49

# Database Configuration
DATABASE_URL=your_postgresql_url

# Logging Configuration
LOG_LEVEL=DEBUG

# Rate Limiting Settings
MAX_REQUESTS_PER_SECOND=10
BURST_LIMIT=500
RATE_LIMIT_COOLDOWN=60

# Wallet Monitoring Settings
WALLET_CHECK_INTERVAL=60
MIN_ADA_BALANCE=5
MAX_TX_PER_HOUR=10
MONITORING_INTERVAL=60
```

### Local Development
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up PostgreSQL database
4. Configure `.env` file with required variables
5. Run: `python bot.py`

### Heroku Deployment
1. Create new Heroku app
2. Add PostgreSQL addon
3. Configure environment variables in Heroku dashboard
4. Deploy using Heroku Git or GitHub integration
5. Ensure worker dyno is enabled

## Architecture

### Database Schema
- Wallet tracking
- User settings
- Transaction history
- Token balances
- Notification preferences

### API Integration
- Blockfrost for Cardano blockchain data
- Discord for bot commands and notifications
- PostgreSQL for persistent storage

### Rate Limiting
- 10 requests/second base rate
- 500 request burst limit
- 60-second cooldown period
- Automatic request queuing

## Support

For issues, questions, or contributions:
1. Open an issue for bugs/features
2. Submit pull requests for improvements
3. Contact admin through Discord for urgent issues

## License

This project is licensed under the MIT License. See LICENSE file for details.