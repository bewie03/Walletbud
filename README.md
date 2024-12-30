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