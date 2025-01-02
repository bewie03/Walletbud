from aiohttp import web
from bot import WalletBudBot

# Create aiohttp app
app = web.Application()

# Initialize bot instance
bot = WalletBudBot()

# Add webhook route
app.router.add_post('/webhook', bot.handle_webhook)
