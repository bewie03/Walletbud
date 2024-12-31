# WalletBud Discord Bot

A Discord bot for monitoring Cardano wallets with comprehensive transaction tracking and database maintenance.

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
- Rate-limited API requests
- Automatic retry with exponential backoff
- Admin channel error reporting
- IP-based rate limiting for webhooks
- Configurable webhook queue size

### Database Optimization
- Automated schema migrations
- Table partitioning for efficient data management
- Optimized indexes for fast queries
- Connection pooling for better performance
- Parallel query execution support
- Configurable PostgreSQL settings
- Automated maintenance tasks:
  - Transaction archiving
  - Old data cleanup
  - Index optimization
  - Table vacuuming

## Commands

- `/addwallet <address>` - Register a wallet (DM only)
- `/removewallet <address>` - Unregister a wallet
- `/listwallets` - View your registered wallets
- `/balance` - Check wallet balances
- `/notifications` - View notification settings
- `/toggle <type>` - Toggle specific notifications
- `/health` - Check bot status
- `/help` - Show all commands

## Environment Variables

### Required
- `DISCORD_TOKEN` - Discord bot token
- `BLOCKFROST_PROJECT_ID` - Blockfrost API project ID
- `DATABASE_URL` - PostgreSQL connection string

### Optional
- `ADMIN_CHANNEL_ID` - Channel for admin notifications
- `BLOCKFROST_BASE_URL` - Custom Blockfrost API URL
- `MAX_REQUESTS_PER_SECOND` - API rate limit (default: 10)
- `BURST_LIMIT` - API burst limit (default: 500)
- `RATE_LIMIT_COOLDOWN` - Rate limit cooldown in seconds (default: 60)
- `MAX_QUEUE_SIZE` - Maximum webhook queue size (default: 10000)
- `MAX_RETRIES` - Maximum retry attempts (default: 5)
- `MAX_EVENT_AGE` - Maximum event age in seconds (default: 3600)
- `BATCH_SIZE` - Process batch size (default: 10)
- `MAX_WEBHOOK_SIZE` - Maximum webhook size in bytes (default: 1MB)
- `WEBHOOK_RATE_LIMIT` - Maximum webhooks per minute (default: 100)
- `PROCESS_INTERVAL` - Queue process interval in seconds (default: 1)
- `MAX_ERROR_HISTORY` - Maximum errors to keep in history (default: 1000)
- `WALLET_CHECK_INTERVAL` - Wallet check interval in seconds (default: 300)
- `MIN_ADA_BALANCE` - Minimum ADA balance for alerts (default: 5)
- `MAX_TX_PER_HOUR` - Maximum transactions per hour (default: 10)
- `MAINTENANCE_HOUR` - Hour to run maintenance (default: 2)
- `MAINTENANCE_MINUTE` - Minute to run maintenance (default: 0)
- `ARCHIVE_AFTER_DAYS` - Days before archiving transactions (default: 90)
- `DELETE_AFTER_DAYS` - Days before deleting archived data (default: 180)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/walletbud.git
cd walletbud
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a .env file with required environment variables:
```env
DISCORD_TOKEN=your_discord_token
BLOCKFROST_PROJECT_ID=your_blockfrost_project_id
DATABASE_URL=postgresql://user:password@localhost:5432/walletbud
```

4. Initialize the database:
```bash
python -c "from database import init_db; import asyncio; asyncio.run(init_db())"
```

5. Start the bot:
```bash
python bot.py
```

## Architecture

### Components
- `bot.py` - Main Discord bot implementation
- `database.py` - Database operations and connection management
- `database_maintenance.py` - Database maintenance and optimization
- `webhook_queue.py` - Webhook processing and rate limiting
- `config.py` - Configuration management

### Data Flow
1. Discord interactions trigger bot commands
2. Commands interact with Blockfrost API for blockchain data
3. Data is stored in PostgreSQL database
4. Webhooks notify of blockchain events
5. Events are queued and processed with rate limiting
6. Database maintenance runs periodically to optimize performance

### Security Measures
- All sensitive commands are DM-only
- Rate limiting on API requests and webhooks
- IP-based rate limiting for webhook endpoints
- Configurable queue sizes and timeouts
- Error tracking and admin notifications

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests (if available)
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue on GitHub or contact the maintainers.