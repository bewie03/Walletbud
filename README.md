# WalletBud Discord Bot

A Discord bot for monitoring Cardano wallets and YUMMI token transactions.

## Features

- Monitor Cardano wallets for transactions
- Track YUMMI token balances
- Receive DM notifications for:
  - New transactions
  - YUMMI balance changes
  - Wallet status updates

## Commands

- `/addwallet` - Add a Cardano wallet (requires 20,000 YUMMI tokens)
- `/list_wallets` - View your registered wallets
- `/remove_wallet` - Remove a wallet from tracking

## Requirements

- Python 3.11.7
- Discord Bot Token
- Blockfrost API Key
- PostgreSQL database (Heroku) or SQLite (local)

## Environment Variables

Create a `.env` file with:
```env
DISCORD_TOKEN=your_discord_bot_token
BLOCKFROST_API_KEY=your_blockfrost_api_key
YUMMI_POLICY_ID=078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924
```

## Configuration

The bot can be configured using environment variables:

- `DISCORD_TOKEN`: Your Discord bot token
- `BLOCKFROST_API_KEY`: Your Blockfrost API key
- `YUMMI_POLICY_ID`: The policy ID for YUMMI tokens
- `REQUIRED_BUD_TOKENS`: Number of YUMMI tokens required (default: 1)
- `TRANSACTION_CHECK_INTERVAL`: Minutes between wallet checks (default: 5)
- `YUMMI_CHECK_INTERVAL`: Hours between YUMMI balance checks (default: 6)
- `MAX_TX_HISTORY`: Maximum number of transactions to check per wallet (default: 10)

## API Usage Optimization

The bot implements several optimizations to reduce API usage:

1. **YUMMI Balance Checks**: Only checks YUMMI balance every 6 hours instead of every transaction check
2. **Transaction History Caching**: Stores the last seen transaction hash to avoid processing the same transactions multiple times
3. **Rate Limit Management**: 
   - Processes wallets in batches of 20
   - Adds small delays between wallet checks
   - Skips overlapping checks if previous batch is still processing

For 10,000 wallets, estimated daily API calls:
- YUMMI Balance Checks: ~40,000 calls (4 checks per day)
- Transaction Monitoring: ~4.3M calls
- Total: ~4.35M calls per day

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the bot:
```bash
python bot.py
```

## Deployment

The bot is configured for Heroku deployment with:
- Procfile for worker dyno
- PostgreSQL database support
- Runtime.txt specifying Python version