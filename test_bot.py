import os
import asyncio
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from blockfrost import BlockFrostApi
from database import (
    get_notification_settings,
    update_notification_setting,
    should_notify,
    initialize_notification_settings,
    init_db,
    add_wallet,
    update_ada_balance,
    update_token_balances,
    update_delegation_status,
    add_processed_reward,
    add_transaction,
    update_policy_expiry,
    update_dapp_interaction,
    get_wallet_id,
    get_stake_address,
    add_failed_transaction,
    add_asset_history,
    remove_wallet,
    create_pool,
    get_db_version,
    run_migrations,
    get_pool
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
BLOCKFROST_PROJECT_ID = os.getenv('BLOCKFROST_PROJECT_ID')
YUMMI_POLICY_ID = "078eafce5cd7edafdf63900edef2c1ea759e77f30ca81d6bbdeec9247"
YUMMI_TOKEN_NAME = "9756d6d69"
YUMMI_TOKEN_ID = YUMMI_POLICY_ID + YUMMI_TOKEN_NAME

async def check_wallet(address):
    try:
        client = BlockFrostApi(project_id=BLOCKFROST_PROJECT_ID)
        
        # Get UTXOs for both ADA and YUMMI balance
        utxos = await asyncio.to_thread(client.address_utxos, address)
        
        # Calculate total ADA
        total_ada = sum(
            int(amount.quantity) / 1000000 
            for utxo in utxos 
            for amount in utxo.amount 
            if amount.unit == 'lovelace'
        )
        
        # Calculate YUMMI balance
        yummi_amount = sum(
            int(amount.quantity)
            for utxo in utxos
            for amount in utxo.amount
            if amount.unit == YUMMI_TOKEN_ID
        )
        
        logger.info(f"\nWallet Balance:")
        logger.info(f"ADA: {total_ada:,.6f} ₳")
        logger.info(f"YUMMI: {yummi_amount:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error checking wallet: {str(e)}")
        return False

async def should_notify(user_id: str, notification_type: str) -> bool:
    """Check if a notification type is enabled for a user"""
    try:
        settings = await get_notification_settings(user_id)
        return settings.get(notification_type, True)
    except Exception as e:
        logger.error(f"Error checking notification settings: {str(e)}")
        return True  # Default to sending notifications if there's an error

async def test_notifications(user_id: str):
    """Test notification settings"""
    try:
        # First, initialize notification settings if needed
        await initialize_notification_settings(user_id)
        
        # Get current notification settings
        settings = await get_notification_settings(user_id)
        
        logger.info("\nCurrent Notification Settings:")
        for setting, enabled in settings.items():
            logger.info(f"{setting}: {'Enabled' if enabled else 'Disabled'}")
            
        # Test each notification type
        notification_types = [
            'ada_transactions',
            'token_changes',
            'nft_updates',
            'staking_rewards',
            'stake_changes',
            'policy_expiry',
            'delegation_status',
            'dapp_interactions',
            'failed_transactions',
            'asset_history'
        ]
        
        logger.info("\nTesting notifications:")
        for ntype in notification_types:
            try:
                should_send = await should_notify(user_id, ntype)
                logger.info(f"{ntype}: {'Will notify' if should_send else 'Will not notify'}")
            except Exception as e:
                logger.error(f"Error testing notification {ntype}: {str(e)}")
            
        # Test toggling notifications
        test_type = 'ada_transactions'
        current = settings.get(test_type, True)
        await update_notification_setting(user_id, test_type, not current)
        new_settings = await get_notification_settings(user_id)
        logger.info(f"\nToggled {test_type}: {current} -> {new_settings.get(test_type)}")
        
        # Reset back to original
        await update_notification_setting(user_id, test_type, current)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing notifications: {str(e)}")
        return False

async def test_real_notification_scenarios(user_id: str, address: str):
    """Test all notification scenarios with real blockchain data"""
    try:
        logger.info("\n=== Testing Real Blockchain Notification Scenarios ===")
        client = BlockFrostApi(project_id=BLOCKFROST_PROJECT_ID)
        
        # Add wallet to monitor
        await add_wallet(user_id, address)
        wallet_id = await get_wallet_id(user_id, address)
        
        if not wallet_id:
            raise Exception("Failed to get wallet ID")
            
        # 1. Test ADA transaction notification
        logger.info("\n1. Testing ADA transaction notification...")
        transactions = await asyncio.to_thread(client.address_transactions, address)
        if transactions:
            tx = transactions[0]
            tx_details = await asyncio.to_thread(client.transaction, tx.tx_hash)
            metadata = {"amount": tx_details.output_amount[0].quantity}
            await add_transaction(wallet_id, tx.tx_hash, metadata)
            logger.info(f"✓ Added real transaction: {tx.tx_hash}")
        
        # 2. Test token changes notification
        logger.info("\n2. Testing token changes notification...")
        # Get all UTXOs and extract token balances
        utxos = await asyncio.to_thread(client.address_utxos, address)
        token_balances = {}
        for utxo in utxos:
            for amount in utxo.amount:
                if amount.unit != 'lovelace':  # Skip ADA
                    if amount.unit in token_balances:
                        token_balances[amount.unit] += int(amount.quantity)
                    else:
                        token_balances[amount.unit] = int(amount.quantity)
        
        await update_token_balances(address, token_balances)
        logger.info(f"✓ Updated real token balances: {len(token_balances)} tokens")
        
        # 3. Test NFT updates
        logger.info("\n3. Testing NFT updates notification...")
        # NFTs are included in token_balances above
        nft_count = sum(1 for unit in token_balances.keys() if len(unit) > 56)
        logger.info(f"✓ Found {nft_count} NFTs in wallet")
        
        # 4. Test staking rewards
        logger.info("\n4. Testing staking rewards notification...")
        stake_addr = await get_stake_address(address)
        if stake_addr:
            rewards = await asyncio.to_thread(client.account_rewards, stake_addr)
            if rewards:
                reward = rewards[0]
                await add_processed_reward(stake_addr, reward.epoch, reward.amount)
                logger.info(f"✓ Added real staking reward: {reward.amount/1000000} ADA from epoch {reward.epoch}")
        
        # 5. Test stake changes
        logger.info("\n5. Testing stake changes notification...")
        if stake_addr:
            pool = await asyncio.to_thread(client.account_delegations, stake_addr)
            if pool:
                await update_delegation_status(address, pool[0].pool_id)
                logger.info(f"✓ Updated real delegation status: {pool[0].pool_id}")
        
        # 6. Test policy expiry
        logger.info("\n6. Testing policy expiry notification...")
        tip = await asyncio.to_thread(client.block_latest)
        if tip:
            expiry_slot = tip.slot + 1000000  # Set expiry 1M slots in future
            await update_policy_expiry(YUMMI_POLICY_ID, expiry_slot)
            logger.info(f"✓ Set policy expiry: slot {expiry_slot}")
        
        # 7. Test DApp interaction
        logger.info("\n7. Testing DApp interaction notification...")
        # Look for transactions with metadata (potential DApp interactions)
        if transactions:
            for tx in transactions[:5]:  # Check first 5 transactions
                tx_details = await asyncio.to_thread(client.transaction_metadata, tx.tx_hash)
                if tx_details:
                    await update_dapp_interaction(address, tx.tx_hash)
                    logger.info(f"✓ Found DApp interaction in tx: {tx.tx_hash}")
                    break
        
        # 8. Test failed transactions
        logger.info("\n8. Testing failed transaction notification...")
        if transactions:
            tx = transactions[0]
            tx_details = await asyncio.to_thread(client.transaction, tx.tx_hash)
            
            # Test different error scenarios
            error_scenarios = [
                {
                    "type": "time_constraint",
                    "condition": hasattr(tx_details, 'invalid_before') and tx_details.invalid_before,
                    "details": json.dumps({
                        "invalid_before": str(getattr(tx_details, 'invalid_before', '')),
                        "invalid_hereafter": str(getattr(tx_details, 'invalid_hereafter', ''))
                    })
                },
                {
                    "type": "script_failure",
                    "condition": hasattr(tx_details, 'script_failure') and tx_details.script_failure,
                    "details": json.dumps({
                        "script_error": "Script execution failed",
                        "failure_type": "validation_error"
                    })
                }
            ]
            
            # Add a simulated failed transaction for testing
            await add_failed_transaction(
                wallet_id,
                tx.tx_hash,
                "insufficient_funds",
                json.dumps({
                    "required": "1000000000",
                    "available": "500000000",
                    "error": "Insufficient funds for transaction"
                })
            )
            logger.info(f"✓ Added simulated failed transaction analysis for: {tx.tx_hash}")
            
            for scenario in error_scenarios:
                if scenario["condition"]:
                    await add_failed_transaction(
                        wallet_id,
                        tx.tx_hash,
                        scenario["type"],
                        scenario["details"]
                    )
                    logger.info(f"✓ Added failed transaction analysis for: {tx.tx_hash} (Type: {scenario['type']})")
        
        # 9. Test asset history
        logger.info("\n9. Testing asset history tracking...")
        if token_balances:
            first_token = next(iter(token_balances))
            if transactions:
                tx = transactions[0]
                metadata = json.dumps({
                    "first_seen": datetime.now().isoformat(),
                    "token_name": first_token[56:] if len(first_token) > 56 else first_token,
                    "policy_id": first_token[:56] if len(first_token) > 56 else None,
                    "action_type": "transfer_in"
                })
                
                await add_asset_history(
                    wallet_id=wallet_id,
                    asset_id=first_token,
                    tx_hash=tx.tx_hash,
                    action="transfer_in",
                    quantity=float(token_balances[first_token]),
                    metadata=metadata
                )
                logger.info(f"✓ Added asset history for token: {first_token[:20]}...")
        
        logger.info("\n✓ All real blockchain notification scenarios tested!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing real notification scenarios: {str(e)}")
        logger.error(f"Error details: {str(e.__dict__)}")
        return False

async def test_edge_cases(user_id: str, address: str):
    """Test edge cases and error scenarios"""
    logger.info("\n=== Testing Edge Cases ===")
    
    try:
        # 1. Invalid wallet address format
        logger.info("\n1. Testing invalid wallet address...")
        invalid_address = "invalid_address_format"
        success = await add_wallet(user_id, invalid_address)
        assert not success, "Should reject invalid address format"
        logger.info("✓ Invalid address rejected")
        
        # 2. Duplicate wallet registration
        logger.info("\n2. Testing duplicate wallet registration...")
        await add_wallet(user_id, address)
        success = await add_wallet(user_id, address)
        assert not success, "Should reject duplicate wallet"
        logger.info("✓ Duplicate wallet rejected")
        
        # 3. Non-existent wallet removal
        logger.info("\n3. Testing non-existent wallet removal...")
        success = await remove_wallet(user_id, "addr1nonexistent")
        assert not success, "Should fail to remove non-existent wallet"
        logger.info("✓ Non-existent wallet removal handled")
        
        # 4. Rate limit testing
        logger.info("\n4. Testing rate limits...")
        client = BlockFrostApi(project_id=BLOCKFROST_PROJECT_ID)
        tasks = []
        for _ in range(15):  # Exceed rate limit
            tasks.append(asyncio.create_task(
                asyncio.to_thread(client.address_utxos, address)
            ))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        rate_limited = any(isinstance(r, Exception) for r in results)
        assert rate_limited, "Should hit rate limits"
        logger.info("✓ Rate limits working")
        
        # 5. Database connection loss simulation
        logger.info("\n5. Testing database connection loss...")
        # Simulate by trying to connect to invalid database
        try:
            async with create_pool("postgresql://invalid") as pool:
                pass
            assert False, "Should fail with invalid database"
        except Exception as e:
            logger.info("✓ Database connection error handled")
        
        # 6. Large transaction history
        logger.info("\n6. Testing large transaction history...")
        transactions = await asyncio.to_thread(
            client.address_transactions, 
            "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
        )
        assert len(transactions) > 0, "Should handle large transaction history"
        logger.info(f"✓ Processed {len(transactions)} transactions")
        
        # 7. Concurrent wallet updates
        logger.info("\n7. Testing concurrent updates...")
        update_tasks = []
        for _ in range(5):
            update_tasks.append(asyncio.create_task(
                update_ada_balance(address, 1000000)
            ))
        await asyncio.gather(*update_tasks)
        logger.info("✓ Concurrent updates handled")
        
        logger.info("\n✓ All edge cases tested successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in edge case testing: {str(e)}")
        return False

async def test_database_migrations():
    """Test database migration system"""
    try:
        # Get initial version
        version = await get_db_version()
        logger.info(f"Initial database version: {version}")
        
        # Run migrations
        success = await run_migrations()
        assert success, "Failed to run migrations"
        
        # Verify version updated
        new_version = await get_db_version()
        assert new_version == CURRENT_VERSION, f"Version mismatch: {new_version} != {CURRENT_VERSION}"
        
        # Verify required tables
        pool = await get_pool()
        async with pool.acquire() as conn:
            for table in [
                'wallets', 'notification_settings', 'transactions', 'token_balances',
                'delegation_status', 'processed_rewards', 'policy_expiry', 'dapp_interactions',
                'failed_transactions', 'asset_history', 'db_version'
            ]:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = $1
                    )
                """, table)
                assert exists, f"Missing required table: {table}"
                
            # Verify indexes
            for index in [
                'idx_wallets_user_id', 'idx_wallets_address',
                'idx_transactions_wallet_id', 'idx_transactions_created_at',
                'idx_token_balances_address', 'idx_delegation_status_address',
                'idx_processed_rewards_stake_address', 'idx_policy_expiry_policy_id',
                'idx_dapp_interactions_address', 'idx_failed_transactions_wallet_id',
                'idx_asset_history_wallet_id'
            ]:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM pg_indexes 
                        WHERE schemaname = 'public' 
                        AND indexname = $1
                    )
                """, index)
                assert exists, f"Missing required index: {index}"
                
            # Verify partitioning
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_partitioned_table pt
                    JOIN pg_class pc ON pt.partrelid = pc.oid
                    WHERE pc.relname = 'transactions_by_month'
                )
            """)
            assert exists, "Transactions table not partitioned"
            
            # Verify partitions exist
            partitions = await conn.fetch("""
                SELECT child.relname AS partition_name
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname = 'transactions_by_month'
            """)
            assert len(partitions) >= 12, "Missing monthly partitions"
            
        logger.info("Database migration tests passed")
        return True
        
    except Exception as e:
        logger.error(f"Database migration test failed: {str(e)}")
        return False

async def test_database_scaling():
    """Test database scaling features"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Test parallel query execution
            await conn.execute("SET max_parallel_workers_per_gather = 4")
            await conn.execute("SET max_parallel_workers = 8")
            
            # Verify settings
            parallel_workers = await conn.fetchval(
                "SHOW max_parallel_workers_per_gather"
            )
            assert int(parallel_workers) == 4, "Parallel workers not set correctly"
            
            # Test connection pool settings
            await conn.execute("SHOW max_connections")
            await conn.execute("SHOW shared_buffers")
            await conn.execute("SHOW effective_cache_size")
            
            # Test transaction partitioning
            # Add some test transactions across different months
            for i in range(3):
                await conn.execute("""
                    INSERT INTO transactions_by_month (
                        wallet_id, tx_hash, metadata, created_at
                    ) VALUES (
                        1, 
                        $1,
                        '{"test": true}',
                        CURRENT_DATE + ($2 || ' months')::interval
                    )
                """, f'test_tx_{i}', i)
            
            # Verify transactions went to correct partitions
            for i in range(3):
                count = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM transactions_by_month
                    WHERE tx_hash = $1
                """, f'test_tx_{i}')
                assert count == 1, f"Transaction {i} not found in partition"
            
            # Clean up test data
            await conn.execute("""
                DELETE FROM transactions_by_month 
                WHERE tx_hash LIKE 'test_tx_%'
            """)
            
        logger.info("Database scaling tests passed")
        return True
        
    except Exception as e:
        logger.error(f"Database scaling test failed: {str(e)}")
        return False

async def main():
    """Run all tests"""
    try:
        # Initialize database first
        await init_db()
        
        # Test database migrations and scaling
        assert await test_database_migrations(), "Database migration tests failed"
        assert await test_database_scaling(), "Database scaling tests failed"
        
        # Test user and wallet
        test_user_id = "test_user_123"
        test_address = "addr1qxqs59lphg8g6qndelq8xwqn60ag3aeyfcp33c2kdp46a09re5df3pzwwmyq946axfcejy5n4x0y99wqpgtp2gd0k09qsgy6pz"
        
        # Run all tests
        await test_notifications(test_user_id)
        await test_real_notification_scenarios(test_user_id, test_address)
        await test_edge_cases(test_user_id, test_address)
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
