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

- `/addwallet` - Add a Cardano wallet (DM only, requires 25,000 YUMMI tokens)
- `/removewallet` - Remove a wallet from tracking
- `/listwallets` - View your registered wallets
- `/health` - Check bot's health status
- `/togglemonitor` - Pause/resume wallet monitoring
- `/help` - Show available commands

## Requirements

- Python 3.11.7
- Discord Bot Token
- Blockfrost API Key
- SQLite database

## Environment Variables

Create a `.env` file with:
```env
DISCORD_TOKEN=your_discord_bot_token
BLOCKFROST_API_KEY=your_blockfrost_api_key
YUMMI_POLICY_ID=078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924
YUMMI_ASSET_NAME=59554D4D49  # hex-encoded "YUMMI"
```

## Configuration

The bot can be configured using environment variables:

- `DISCORD_TOKEN`: Your Discord bot token
- `BLOCKFROST_API_KEY`: Your Blockfrost API key
- `YUMMI_POLICY_ID`: The policy ID for YUMMI tokens (078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec924)
- `YUMMI_ASSET_NAME`: The hex-encoded asset name (59554D4D49)
- `REQUIRED_YUMMI_TOKENS`: Number of YUMMI tokens required (default: 25,000)
- `TRANSACTION_CHECK_INTERVAL`: Minutes between wallet checks (default: 5)
- `YUMMI_CHECK_INTERVAL`: Hours between YUMMI balance checks (default: 6)
- `MAX_TX_HISTORY`: Maximum number of transactions to check per wallet (default: 10)

## API Usage Optimization

The bot implements several optimizations to reduce API usage:

1. **YUMMI Balance Checks**: Only checks YUMMI balance every 6 hours instead of every transaction check
2. **Transaction History Caching**: Stores the last seen transaction hash to avoid processing the same transactions multiple times
3. **Rate Limit Management**: 
   - Adds small delays between wallet checks
   - Skips overlapping checks if previous batch is still processing
   - Proper error handling for rate limits

## Error Handling

The bot includes comprehensive error handling:
- API rate limits
- Network errors
- Invalid wallet addresses
- Database errors
- Discord API issues

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables in `.env`

3. Run the bot:
```bash
python bot.py
```

## Development

The bot is built with:
- discord.py for Discord integration
- Blockfrost-python for Cardano blockchain interaction
- SQLite for data persistence
- Proper async/await patterns for optimal performance

## Security

- Wallet commands only work in DMs
- Environment variables for sensitive data
- Input validation for wallet addresses
- Rate limiting and error handling
- No sensitive data in logs