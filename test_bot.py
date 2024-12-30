import os
import asyncio
import logging
import discord
from discord.ext import commands
from dotenv import load_dotenv
from config import (
    COMMAND_PREFIX,
    REQUIRED_YUMMI_TOKENS,
    YUMMI_POLICY_ID
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix=COMMAND_PREFIX, intents=intents)
        
        # Register commands
        self.setup_commands()
        
    def setup_commands(self):
        """Set up test commands"""
        @self.command(name='ping')
        async def ping(ctx):
            """Test basic command functionality"""
            try:
                await ctx.send('Pong!')
                logger.info("Ping command successful")
            except Exception as e:
                logger.error(f"Ping command failed: {str(e)}")
                
        @self.command(name='config')
        async def config(ctx):
            """Test configuration values"""
            try:
                embed = discord.Embed(
                    title="Bot Configuration",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Command Prefix", value=COMMAND_PREFIX, inline=True)
                embed.add_field(name="Required YUMMI", value=REQUIRED_YUMMI_TOKENS, inline=True)
                embed.add_field(name="YUMMI Policy ID", value=YUMMI_POLICY_ID, inline=True)
                
                await ctx.send(embed=embed)
                logger.info("Config command successful")
            except Exception as e:
                logger.error(f"Config command failed: {str(e)}")
                
    async def setup_hook(self):
        """Setup hook for additional initialization"""
        try:
            # Sync commands
            await self.tree.sync()
            logger.info("Commands synced successfully")
        except Exception as e:
            logger.error(f"Failed to sync commands: {str(e)}")
            
    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot latency: {round(self.latency * 1000)}ms')
        
    async def on_command_error(self, ctx, error):
        """Handle command errors"""
        if isinstance(error, commands.errors.CommandNotFound):
            await ctx.send("Command not found!")
        elif isinstance(error, commands.errors.MissingPermissions):
            await ctx.send("You don't have permission to use this command!")
        else:
            logger.error(f"Command error: {str(error)}")
            await ctx.send(f"An error occurred: {str(error)}")

async def main():
    """Main entry point"""
    try:
        # Load environment variables
        if not load_dotenv():
            logger.error("Failed to load .env file")
            return
            
        # Get Discord token
        token = os.getenv('DISCORD_TOKEN')
        if not token:
            logger.error("No Discord token found! Make sure DISCORD_TOKEN is set in .env")
            return
            
        # Initialize and run bot
        bot = TestBot()
        async with bot:
            await bot.start(token)
            
    except Exception as e:
        logger.error(f"Bot startup failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
