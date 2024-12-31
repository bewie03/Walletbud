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

### Database Optimization
- Automated schema migrations
- Table partitioning for efficient data management
- Optimized indexes for fast queries
- Connection pooling for better performance
- Parallel query execution support
- Configurable PostgreSQL settings

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

### YUMMI Token Details
The YUMMI token can be represented in different formats across different platforms:

1. **Blockfrost API Format** (Used by the bot):
   - Policy ID: `078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924`
   - Token Name (hex): `9756d6d69`
   - Full Token ID: `078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec9249756d6d69`

2. **Pool.pm Format**:
   - Asset ID: `asset1jhsvtq7uaz0npx5vryc5um3r34tmz576qe3kkj`

These are different representations of the same token. The bot uses the Blockfrost API format since it interacts with the Blockfrost API. When viewing the token on pool.pm or other blockchain explorers, you might see the Asset ID format instead.

### Environment Variables

### Database Configuration
- `DATABASE_URL`: PostgreSQL connection string
- `BLOCKFROST_PROJECT_ID`: Blockfrost API project ID
- `BLOCKFROST_BASE_URL`: Blockfrost API base URL
- `ADMIN_CHANNEL_ID`: Discord channel ID for admin notifications

### Webhook Security
- `BLOCKFROST_IP_RANGES`: Allowed IP ranges for Blockfrost webhooks
- `WEBHOOK_RATE_LIMIT`: Maximum webhooks per minute
- `MAX_WEBHOOK_SIZE`: Maximum webhook payload size in bytes
- `RATE_LIMIT_WINDOW`: Rate limit window in seconds (default: 60)
- `RATE_LIMIT_MAX_REQUESTS`: Maximum requests per window per IP (default: 100)

### Database Maintenance
- `ARCHIVE_AFTER_DAYS`: Days before archiving transactions (default: 90)
- `DELETE_AFTER_DAYS`: Days before deleting archived transactions (default: 180)
- `MAINTENANCE_BATCH_SIZE`: Number of records to process in each batch (default: 1000)
- `MAINTENANCE_MAX_RETRIES`: Maximum retries for failed maintenance operations (default: 3)
- `MAINTENANCE_HOUR`: Hour to run maintenance in 24-hour format (default: 2)
- `MAINTENANCE_MINUTE`: Minute to run maintenance (default: 0)

### Queue Configuration
- `MAX_QUEUE_SIZE`: Maximum events in queue (default: 10000)
- `MAX_RETRIES`: Maximum retry attempts per event (default: 5)
- `MAX_EVENT_AGE`: Maximum age of event in seconds (default: 3600)
- `BATCH_SIZE`: Number of events to process in each batch (default: 10)
- `PROCESS_INTERVAL`: Process queue interval in seconds (default: 1)
- `MAX_ERROR_HISTORY`: Maximum number of errors to keep in history (default: 1000)

### Database Setup
1. Create a PostgreSQL database
2. Set `DATABASE_URL` environment variable
3. Run migrations: `python database.py`

The bot uses an automated migration system to manage database schema changes. Migrations are versioned and run automatically during startup. The system includes:

- Version tracking table
- Automated schema updates
- Table partitioning for transactions
- Index creation for performance
- Connection pool optimization

### Performance Tuning
The database is optimized for high performance with:

- Monthly partitioning for transaction history
- Indexes on frequently queried columns
- Connection pooling (max 200 connections)
- Parallel query execution (4-8 workers)
- Optimized PostgreSQL settings for SSD storage

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