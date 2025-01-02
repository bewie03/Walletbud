"""
Database maintenance module for WalletBud.
Handles database cleanup, archiving, and optimization tasks.
"""

import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from database import (
    get_pool,
    execute_many
)
from config import (
    ARCHIVE_AFTER_DAYS,
    DELETE_AFTER_DAYS,
    MAINTENANCE_BATCH_SIZE as BATCH_SIZE,
    MAINTENANCE_MAX_RETRIES as MAX_RETRIES,
    MAINTENANCE_HOUR,
    MAINTENANCE_MINUTE
)

logger = logging.getLogger(__name__)

class DatabaseMaintenanceError(Exception):
    """Base exception for database maintenance errors"""
    pass

class DatabaseMaintenance:
    """Handles database maintenance tasks"""
    def __init__(self):
        """Initialize database maintenance"""
        self._maintenance_lock = None
        self._is_maintaining = False
        self._last_maintenance: Optional[datetime] = None
        self._maintenance_stats: Dict[str, Any] = {}
        self._pool = None
        logger.info("DatabaseMaintenance initialized")

    async def _ensure_lock(self):
        """Ensure maintenance lock is created"""
        if self._maintenance_lock is None:
            self._maintenance_lock = asyncio.Lock()
        return self._maintenance_lock

    async def _get_pool(self):
        """Get database pool, initializing if necessary"""
        if not self._pool:
            self._pool = await get_pool()
        return self._pool

    async def start_maintenance_task(self):
        """Start the periodic maintenance task"""
        while True:
            try:
                # Get current hour in UTC
                current_hour = datetime.utcnow().hour
                
                # Only run maintenance between 2 AM and 4 AM UTC (low activity period)
                if 2 <= current_hour <= 4:
                    logger.info("Starting scheduled maintenance during low-activity period")
                    await self.run_maintenance()
                    # Sleep for 6 hours after maintenance
                    await asyncio.sleep(6 * 3600)
                else:
                    # Check again in an hour
                    await asyncio.sleep(3600)
                    
            except Exception as e:
                logger.error(f"Error in maintenance task: {str(e)}")
                if hasattr(e, '__dict__'):
                    logger.error(f"Error details: {e.__dict__}")
                # Sleep for an hour before retrying
                await asyncio.sleep(3600)

    async def run_maintenance(self) -> Dict[str, Any]:
        """Run all maintenance tasks
        
        Returns:
            Dict[str, Any]: Statistics about the maintenance run
        """
        if self._is_maintaining:
            logger.warning("Maintenance already in progress")
            return self._maintenance_stats
        
        async with await self._ensure_lock():
            try:
                self._is_maintaining = True
                start_time = datetime.now()
                self._maintenance_stats = {
                    'start_time': start_time,
                    'end_time': None,
                    'duration': None,
                    'archived_transactions': 0,
                    'deleted_transactions': 0,
                    'optimized_tables': 0,
                    'errors': []
                }
                
                pool = await self._get_pool()
                async with pool.acquire() as conn:
                    # Run maintenance tasks
                    self._maintenance_stats['archived_transactions'] = await self._archive_old_transactions(conn)
                    self._maintenance_stats['deleted_transactions'] = await self._delete_old_archived_transactions(conn)
                    self._maintenance_stats['optimized_tables'] = await self._optimize_tables(conn)
                    
                end_time = datetime.now()
                self._maintenance_stats['end_time'] = end_time
                self._maintenance_stats['duration'] = (end_time - start_time).total_seconds()
                self._last_maintenance = end_time
                
                logger.info(f"Maintenance completed in {self._maintenance_stats['duration']} seconds")
                return self._maintenance_stats
                
            except Exception as e:
                logger.error(f"Error during maintenance: {e}")
                self._maintenance_stats['errors'].append(str(e))
                raise DatabaseMaintenanceError(f"Maintenance failed: {e}")
                
            finally:
                self._is_maintaining = False

    async def _archive_old_transactions(self, conn) -> int:
        """Archive transactions older than ARCHIVE_AFTER_DAYS
        
        Returns:
            int: Number of transactions archived
        """
        async with await self._ensure_lock():
            total_archived = 0
            cutoff_date = datetime.now() - timedelta(days=ARCHIVE_AFTER_DAYS)
            
            try:
                # Get old transactions in batches
                while True:
                    query = """
                        WITH old_txs AS (
                            SELECT tx_hash 
                            FROM transactions 
                            WHERE created_at < $1
                            AND NOT archived
                            LIMIT $2
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE transactions t
                        SET archived = true,
                            archived_at = NOW()
                        FROM old_txs
                        WHERE t.tx_hash = old_txs.tx_hash
                        RETURNING t.tx_hash
                    """
                    
                    result = await conn.fetch(query, cutoff_date, BATCH_SIZE)
                    
                    if not result:
                        break
                        
                    count = len(result)
                    total_archived += count
                    logger.info(f"Archived {count} transactions")
                    
                    # Small delay between batches
                    await asyncio.sleep(0.1)
                
                return total_archived
                
            except Exception as e:
                logger.error(f"Error archiving transactions: {str(e)}")
                raise DatabaseMaintenanceError(f"Error archiving transactions: {str(e)}")

    async def _delete_old_archived_transactions(self, conn) -> int:
        """Delete archived transactions older than DELETE_AFTER_DAYS
        
        Returns:
            int: Number of transactions deleted
        """
        async with await self._ensure_lock():
            total_deleted = 0
            cutoff_date = datetime.now() - timedelta(days=DELETE_AFTER_DAYS)
            
            try:
                # Delete in batches
                while True:
                    query = """
                        WITH old_archived AS (
                            SELECT tx_hash 
                            FROM transactions 
                            WHERE archived 
                            AND archived_at < $1
                            LIMIT $2
                            FOR UPDATE SKIP LOCKED
                        )
                        DELETE FROM transactions t
                        USING old_archived
                        WHERE t.tx_hash = old_archived.tx_hash
                        RETURNING t.tx_hash
                    """
                    
                    result = await conn.fetch(query, cutoff_date, BATCH_SIZE)
                    
                    if not result:
                        break
                        
                    count = len(result)
                    total_deleted += count
                    logger.info(f"Deleted {count} old archived transactions")
                    
                    # Small delay between batches
                    await asyncio.sleep(0.1)
                
                return total_deleted
                
            except Exception as e:
                logger.error(f"Error deleting old transactions: {str(e)}")
                raise DatabaseMaintenanceError(f"Error deleting old transactions: {str(e)}")

    async def _optimize_tables(self, conn) -> int:
        """Optimize database tables
        
        Returns:
            int: Number of tables optimized
        """
        async with await self._ensure_lock():
            tables_optimized = 0
            
            try:
                # Get list of tables
                tables = await conn.fetch("""
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                    AND tablename NOT LIKE 'pg_%'
                    AND tablename NOT LIKE 'sql_%'
                """)
                
                for table in tables:
                    table_name = table['tablename']
                    try:
                        # Run ANALYZE first
                        await conn.execute(f"ANALYZE {table_name}")
                        logger.info(f"Analyzed table: {table_name}")
                        
                        # Then try VACUUM
                        await conn.execute(f"VACUUM {table_name}")
                        logger.info(f"Vacuumed table: {table_name}")
                        
                        tables_optimized += 1
                        
                    except Exception as e:
                        logger.error(f"Error optimizing table {table_name}: {str(e)}")
                        continue
                        
                    # Small delay between tables
                    await asyncio.sleep(0.1)
                
                return tables_optimized
                
            except Exception as e:
                logger.error(f"Error during table optimization: {str(e)}")
                raise DatabaseMaintenanceError(f"Error during table optimization: {str(e)}")

    async def get_maintenance_stats(self) -> Dict[str, Any]:
        """Get statistics about the last maintenance run
        
        Returns:
            Dict[str, Any]: Maintenance statistics
        """
        async with await self._ensure_lock():
            return {
                'last_run': self._last_maintenance,
                'is_maintaining': self._is_maintaining,
                'stats': self._maintenance_stats
            }

    async def cleanup(self):
        """Cleanup resources"""
        if self._pool:
            await self._pool.close()
            self._pool = None

class DatabaseMaintenanceWorker:
    def __init__(self):
        self.pool = None
        self.running = False
        self.last_maintenance = None
        self.maintenance = DatabaseMaintenance()

    async def init_pool(self):
        """Initialize database pool"""
        if not self.pool:
            self.pool = await get_pool()
            logger.info("Database pool initialized")

    async def run_maintenance(self):
        """Run maintenance tasks"""
        try:
            logger.info("Starting database maintenance")
            async with await self.maintenance._ensure_lock():
                await self.maintenance.run_maintenance()
            self.last_maintenance = datetime.utcnow()
            logger.info("Database maintenance completed successfully")
        except Exception as e:
            logger.error(f"Error during maintenance: {e}")

    async def should_run_maintenance(self):
        """Check if maintenance should run"""
        now = datetime.utcnow()
        
        # If never run, run immediately
        if not self.last_maintenance:
            return True

        # Calculate next scheduled time
        next_run = datetime.utcnow().replace(
            hour=int(MAINTENANCE_HOUR),
            minute=int(MAINTENANCE_MINUTE),
            second=0,
            microsecond=0
        )
        
        # If next run time is in the past, add a day
        if next_run <= now:
            next_run += timedelta(days=1)

        # Run if it's time
        return now >= next_run

    async def start(self):
        """Start the maintenance worker"""
        self.running = True
        await self.init_pool()
        
        logger.info("Database maintenance worker started")
        
        while self.running:
            try:
                if await self.should_run_maintenance():
                    await self.run_maintenance()
                
                # Sleep for 5 minutes before next check
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
                # Sleep for 1 minute on error
                await asyncio.sleep(60)

    def stop(self):
        """Stop the maintenance worker"""
        self.running = False
        logger.info("Database maintenance worker stopped")

async def main():
    """Main entry point"""
    worker = DatabaseMaintenanceWorker()
    try:
        await worker.start()
    except KeyboardInterrupt:
        worker.stop()
    except Exception as e:
        logger.error(f"Fatal error in maintenance worker: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
