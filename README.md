# WalletBud Discord Bot

A Discord bot that monitors Cardano wallet activity and sends notifications for transactions.

## Features

- Track multiple Cardano wallets
- Receive notifications for incoming and outgoing transactions
- Token-gated access requiring 20,000 $BUD tokens per wallet
- Real-time monitoring using Blockfrost API

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with the following variables:
```
DISCORD_TOKEN=your_discord_bot_token
BLOCKFROST_API_KEY=your_blockfrost_api_key
```

3. Run the bot:
```bash
python bot.py
```

## Commands

- `/addwallet [wallet_address]` - Add a wallet for tracking

## Requirements

- Python 3.8+
- Discord Bot Token
- Blockfrost API Key
- 20,000 $BUD tokens per wallet for tracking

## Notes

- The bot checks wallets every 15 minutes for new transactions
- Notifications are sent via Discord DM
- Wallet tracking is automatically disabled if $BUD token balance falls below requirement
