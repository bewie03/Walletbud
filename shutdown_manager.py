import asyncio
import logging
import signal
from typing import Dict, List, Callable, Coroutine, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class ShutdownManager:
    """Manages graceful shutdown of all bot components"""
    
    def __init__(self):
        self.shutdown_handlers: Dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self.is_shutting_down = False
        self.shutdown_start = None
        self._shutdown_lock = asyncio.Lock()
        
    def register_handler(self, name: str, handler: Callable[[], Coroutine[Any, Any, None]]):
        """Register a shutdown handler"""
        self.shutdown_handlers[name] = handler
        logger.debug(f"Registered shutdown handler: {name}")
        
    async def cleanup(self, timeout: float = 30.0):
        """Execute all shutdown handlers with timeout"""
        async with self._shutdown_lock:
            if self.is_shutting_down:
                logger.warning("Shutdown already in progress")
                return
                
            self.is_shutting_down = True
            self.shutdown_start = datetime.utcnow()
            
            logger.info("Starting graceful shutdown...")
            
            # Create tasks for all handlers
            tasks = []
            for name, handler in self.shutdown_handlers.items():
                task = asyncio.create_task(self._execute_handler(name, handler))
                tasks.append(task)
            
            try:
                # Wait for all handlers with timeout
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
                logger.info("Graceful shutdown completed successfully")
                
            except asyncio.TimeoutError:
                logger.error(f"Shutdown timed out after {timeout} seconds")
                # Cancel any remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
                        
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")
                
    async def _execute_handler(self, name: str, handler: Callable[[], Coroutine[Any, Any, None]]):
        """Execute a single shutdown handler with error handling"""
        try:
            logger.debug(f"Executing shutdown handler: {name}")
            await handler()
            logger.debug(f"Shutdown handler completed: {name}")
            
        except Exception as e:
            logger.error(f"Error in shutdown handler {name}: {e}")
            
    def setup_signal_handlers(self, loop: asyncio.AbstractEventLoop):
        """Setup signal handlers for graceful shutdown"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._signal_handler(s))
                )
            logger.info("Signal handlers registered successfully")
            
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
            
    async def _signal_handler(self, sig: signal.Signals):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig.name}")
        await self.cleanup()
