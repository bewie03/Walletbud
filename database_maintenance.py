"""
Database maintenance module for WalletBud.
Handles database cleanup, archiving, and optimization tasks.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from database import (
    get_pool,
    execute_query,
    execute_many,
    fetch_all,
    fetch_one
)

logger = logging.getLogger(__name__)

# Load maintenance configuration from environment
ARCHIVE_AFTER_DAYS = int(os.getenv('ARCHIVE_AFTER_DAYS', '90'))  # Archive transactions older than 90 days
DELETE_AFTER_DAYS = int(os.getenv('DELETE_AFTER_DAYS', '180'))  # Delete archived transactions older than 180 days
BATCH_SIZE = int(os.getenv('MAINTENANCE_BATCH_SIZE', '1000'))  # Process records in batches
MAX_RETRIES = int(os.getenv('MAINTENANCE_MAX_RETRIES', '3'))  # Maximum retries for failed operations
MAINTENANCE_HOUR = int(os.getenv('MAINTENANCE_HOUR', '2'))  # Hour to run maintenance (24-hour format)
MAINTENANCE_MINUTE = int(os.getenv('MAINTENANCE_MINUTE', '0'))  # Minute to run maintenance

class DatabaseMaintenanceError(Exception):
    """Base exception for database maintenance errors"""
    pass

class DatabaseMaintenance:
    """Handles database maintenance tasks"""
    def __init__(self):
        """Initialize database maintenance"""
        self._maintenance_lock = asyncio.Lock()
        self._is_maintaining = False
        self._last_maintenance: Optional[datetime] = None
        self._maintenance_stats: Dict[str, Any] = {}
        self._pool = None

    async def _get_pool(self):
        """Get database pool, initializing if necessary"""
        if not self._pool:
            self._pool = await get_pool()
        return self._pool

    async def start_maintenance_task(self):
        """Start the periodic maintenance task"""
        while True:
            try:
                # Run maintenance at configured time
                now = datetime.now()
                next_run = now.replace(
                    hour=MAINTENANCE_HOUR,
                    minute=MAINTENANCE_MINUTE,
                    second=0,
                    microsecond=0
                )
                if now >= next_run:
                    next_run = next_run + timedelta(days=1)
                
                # Sleep until next maintenance window
                sleep_seconds = (next_run - now).total_seconds()
                logger.info(
                    f"Next maintenance scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"(in {sleep_seconds/3600:.1f} hours)"
                )
                await asyncio.sleep(sleep_seconds)
                
                # Run maintenance
                await self.run_maintenance()
                
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
        
        async with self._maintenance_lock:
            try:
                self._is_maintaining = True
                start_time = datetime.now()
                self._maintenance_stats = {
                    'started_at': start_time,
                    'archived_transactions': 0,
                    'deleted_transactions': 0,
                    'optimized_tables': 0,
                    'errors': []
                }
                
                logger.info("Starting database maintenance")
                pool = await self._get_pool()
                
                # Get a dedicated connection for maintenance
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        try:
                            # Archive old transactions
                            archived = await self._archive_old_transactions(conn)
                            self._maintenance_stats['archived_transactions'] = archived
                            
                            # Delete very old archived transactions
                            deleted = await self._delete_old_archived_transactions(conn)
                            self._maintenance_stats['deleted_transactions'] = deleted
                            
                            # Optimize tables
                            optimized = await self._optimize_tables(conn)
                            self._maintenance_stats['optimized_tables'] = optimized
                            
                        except Exception as e:
                            logger.error(f"Error during maintenance: {str(e)}")
                            self._maintenance_stats['errors'].append(str(e))
                            raise
                
                # Update completion time
                self._maintenance_stats['completed_at'] = datetime.now()
                self._maintenance_stats['duration'] = (
                    self._maintenance_stats['completed_at'] - 
                    self._maintenance_stats['started_at']
                ).total_seconds()
                
                logger.info(
                    f"Maintenance completed in {self._maintenance_stats['duration']:.1f}s: "
                    f"Archived {archived} transactions, "
                    f"Deleted {deleted} old transactions, "
                    f"Optimized {optimized} tables"
                )
                
                return self._maintenance_stats
                
            finally:
                self._is_maintaining = False
                self._last_maintenance = datetime.now()

    async def _archive_old_transactions(self, conn) -> int:
        """Archive transactions older than ARCHIVE_AFTER_DAYS
        
        Returns:
            int: Number of transactions archived
        """
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
                
                result = await execute_query(
                    query,
                    cutoff_date,
                    BATCH_SIZE,
                    fetchall=True,
                    conn=conn
                )
                
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
                
                result = await execute_query(
                    query,
                    cutoff_date,
                    BATCH_SIZE,
                    fetchall=True,
                    conn=conn
                )
                
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
        tables_optimized = 0
        
        try:
            # Get list of tables
            tables = await fetch_all(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
                conn=conn
            )
            
            for table in tables:
                table_name = table['tablename']
                try:
                    # VACUUM ANALYZE each table
                    await execute_query(f"VACUUM ANALYZE {table_name}", conn=conn)
                    tables_optimized += 1
                    logger.info(f"Optimized table: {table_name}")
                except Exception as e:
                    logger.error(f"Error optimizing table {table_name}: {str(e)}")
                    continue
            
            return tables_optimized
            
        except Exception as e:
            logger.error(f"Error during table optimization: {str(e)}")
            raise DatabaseMaintenanceError(f"Error during table optimization: {str(e)}")

    async def get_maintenance_stats(self) -> Dict[str, Any]:
        """Get statistics about the last maintenance run
        
        Returns:
            Dict[str, Any]: Maintenance statistics
        """
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
