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

### Discord Configuration
- `DISCORD_TOKEN` - Discord bot token
- `ADMIN_CHANNEL_ID` - Channel for admin notifications
- `APPLICATION_ID` - Discord application ID

### Blockfrost Configuration
- `BLOCKFROST_PROJECT_ID` - Blockfrost API project ID
- `BLOCKFROST_BASE_URL` - Blockfrost API base URL (default: cardano-mainnet)
- `BLOCKFROST_TX_WEBHOOK_ID` - Transaction webhook ID
- `BLOCKFROST_DEL_WEBHOOK_ID` - Delegation webhook ID
- `BLOCKFROST_WEBHOOK_SECRET` - Webhook secret for verification
- `BLOCKFROST_IP_RANGES` - Allowed IP ranges for webhooks (JSON array)

### Database Configuration
- `DATABASE_URL` - PostgreSQL connection string
- `DB_MIN_POOL_SIZE` - Minimum database pool size (default: 2)
- `DB_MAX_POOL_SIZE` - Maximum database pool size (default: 10)
- `DB_MAX_INACTIVE_CONNECTION_LIFETIME` - Max inactive connection lifetime in seconds (default: 300)
- `DB_COMMAND_TIMEOUT` - Command timeout in seconds (default: 60)

### YUMMI Token Configuration
- `YUMMI_POLICY_ID` - YUMMI token policy ID
- `YUMMI_TOKEN_NAME` - YUMMI token name (hex)
- `MINIMUM_YUMMI` - Minimum YUMMI tokens required (default: 1000)

### Rate Limiting
- `RATE_LIMIT_WINDOW` - Rate limit window in seconds (default: 60)
- `RATE_LIMIT_MAX_REQUESTS` - Max requests per window (default: 100)
- `MAX_RETRIES` - Maximum retry attempts (default: 3)
- `RETRY_DELAY` - Delay between retries in seconds (default: 5)
- `MAX_QUEUE_SIZE` - Maximum webhook queue size (default: 1000)
- `MAX_EVENT_AGE` - Maximum event age in seconds (default: 3600)

### Maintenance Configuration
- `MAINTENANCE_HOUR` - Hour to run maintenance (default: 2)
- `MAINTENANCE_MINUTE` - Minute to run maintenance (default: 0)
- `ARCHIVE_AFTER_DAYS` - Days before archiving transactions (default: 90)
- `DELETE_AFTER_DAYS` - Days before deleting archived data (default: 180)
- `MAINTENANCE_BATCH_SIZE` - Batch size for maintenance tasks (default: 1000)
- `MAINTENANCE_MAX_RETRIES` - Max retries for maintenance tasks (default: 3)

### Health Monitoring
- `HEALTH_CHECK_INTERVAL` - Health check interval in seconds (default: 300)
- `MAX_ERRORS` - Maximum errors before alert (default: 100)
- `ERROR_WINDOW` - Error tracking window in seconds (default: 3600)

### Feature Flags
- `ENABLE_WEBHOOKS` - Enable webhook processing (default: true)
- `ENABLE_MONITORING` - Enable wallet monitoring (default: true)
- `ENABLE_NOTIFICATIONS` - Enable user notifications (default: true)

### Other Settings
- `LOG_LEVEL` - Logging level (default: INFO)
- `COMMAND_COOLDOWN` - Command cooldown in seconds (default: 60)

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

## Database Management

The application uses a PostgreSQL database with a robust migration system. Key features:

- Automatic schema initialization and updates
- Version-controlled migrations
- Error handling and logging
- Support for future schema changes

To make database changes, add a new version to the migrations in `database.py`.

The database initialization is now more robust and can handle future schema changes through the migration system. When you need to make database changes in the future, just add a new migration version to the MIGRATIONS dictionary.