# Blockfrost API Endpoints Reference

This document lists all available methods in the Blockfrost Python SDK version 0.6.0.

**Note**: This reference is accurate as of December 31, 2023, for Blockfrost SDK version 0.6.0. If you upgrade the SDK, please verify the methods against the new version.

## Rate Limiting
Blockfrost implements the following rate limiting rules:
- Base rate: 10 requests per second per IP address
- Burst limit: 500 requests allowed in burst
- Burst cooldown: Burst recharges at 10 requests per second
- Recovery time: After using full burst (500 requests), it takes 50 seconds to fully recharge
- Example: If a user makes a burst of requests, they can make 30 more requests after 3 seconds

## Network Endpoints
Cardano mainnet:  https://cardano-mainnet.blockfrost.io/api/v0
https://cardano-mainnet.blockfrost.io/api/v0/health 

## Account Methods
- `account_addresses` - Get account associated addresses
- `account_addresses_assets` - Get account associated addresses with assets
- `account_addresses_total` - Get account total balance
- `account_delegations` - Get account delegation history
- `account_history` - Get account history
- `account_mirs` - Get account MIR history
- `account_registrations` - Get account registration history
- `account_rewards` - Get account reward history
- `account_withdrawals` - Get account withdrawal history
- `accounts` - Get account information

## Address Methods
- `address` - Get address information
- `address_extended` - Get extended address information
- `address_total` - Get address total balance
- `address_transactions` - Get address transactions
- `address_utxos` - Get address UTXOs
- `address_utxos_asset` - Get address UTXOs filtered by asset
- `assets` - Get address assets

## Asset Methods
- `asset` - Get specific asset information
- `asset_addresses` - Get asset addresses
- `asset_history` - Get asset history
- `asset_transactions` - Get asset transactions
- `assets` - Get all assets
- `assets_policy` - Get assets under policy

## Block Methods
- `block` - Get block information
- `block_epoch_slot` - Get block in epoch slot
- `block_latest` - Get latest block
- `block_latest_transactions` - Get latest block transactions
- `block_slot` - Get block in slot
- `block_transactions` - Get block transactions
- `blocks_addresses` - Get block addresses
- `blocks_next` - Get list of next blocks
- `blocks_previous` - Get list of previous blocks

## Epoch Methods
- `epoch` - Get epoch information
- `epoch_blocks` - Get epoch blocks
- `epoch_latest` - Get latest epoch
- `epoch_latest_parameters` - Get latest epoch protocol parameters
- `epoch_pool_blocks` - Get blocks by pool in epoch
- `epoch_pool_stakes` - Get stake distribution by pool
- `epoch_protocol_parameters` - Get protocol parameters
- `epoch_stakes` - Get stake distribution
- `epochs_next` - Get list of next epochs
- `epochs_previous` - Get list of previous epochs

## Genesis Methods
- `genesis` - Get genesis information

## Health Methods
- `health` - Get health status
- `clock` - Get current backend time

## Metadata Methods
- `metadata_label_cbor` - Get CBOR metadata by label
- `metadata_label_json` - Get JSON metadata by label
- `metadata_labels` - Get metadata labels

## Network Methods
- `network` - Get network information

## Pool Methods
- `pool` - Get pool information
- `pool_blocks` - Get pool blocks
- `pool_delegators` - Get pool delegators
- `pool_history` - Get pool history
- `pool_metadata` - Get pool metadata
- `pool_relays` - Get pool relays
- `pool_updates` - Get pool updates
- `pools` - Get list of pools
- `pools_extended` - Get list of pools with extended information
- `pools_retired` - Get list of retired pools
- `pools_retiring` - Get list of retiring pools

## Script Methods
- `script` - Get script information
- `script_cbor` - Get script CBOR
- `script_datum` - Get script datum
- `script_datum_cbor` - Get script datum CBOR
- `script_json` - Get script JSON
- `script_redeemers` - Get script redeemers
- `scripts` - Get list of scripts

## Transaction Methods
- `transaction` - Get transaction information
- `transaction_delegations` - Get transaction delegation certificates
- `transaction_evaluate` - Submit transaction for execution units evaluation
- `transaction_evaluate_cbor` - Submit CBOR transaction for execution units evaluation
- `transaction_evaluate_utxos` - Submit transaction for execution units evaluation with specific UTXOs
- `transaction_metadata` - Get transaction metadata
- `transaction_metadata_cbor` - Get transaction metadata in CBOR
- `transaction_mirs` - Get transaction MIR certificates
- `transaction_pool_retires` - Get transaction stake pool retirement certificates
- `transaction_pool_updates` - Get transaction stake pool registration/update certificates
- `transaction_redeemers` - Get transaction redeemers
- `transaction_stakes` - Get transaction stake addresses certificates
- `transaction_submit` - Submit transaction
- `transaction_submit_cbor` - Submit CBOR transaction
- `transaction_utxos` - Get transaction UTXOs
- `transaction_withdrawals` - Get transaction withdrawal certificates

## Utility Methods
- `api_version` - Get API version
- `authentication_header` - Get authentication header
- `base_url` - Get base URL
- `default_headers` - Get default headers
- `metrics` - Get metrics
- `metrics_endpoints` - Get endpoint metrics
- `project_id` - Get project ID
- `query_parameters` - Get query parameters
- `root` - Get root endpoint
- `url` - Get URL
- `user_agent_header` - Get user agent header
- `utils_addresses_xpub` - Get addresses from extended public key

## Important Note: Address vs. Addresses Endpoints

### Core Difference
- **Singular (address)**: Operations or details on a specific address
- **Plural (addresses)**: Collections, historical records, or related entities linked to an account's stake address

### Address Endpoints (/addresses/...)
Used for retrieving information about specific wallet addresses:
- `/addresses/{address}` - Get information about a specific address
- `/addresses/{address}/extended` - Get extended details about an address
- `/addresses/{address}/total` - Get the total balance (ADA and assets)
- `/addresses/{address}/utxos` - Get all UTXOs for an address
- `/addresses/{address}/utxos/{asset}` - Get UTXOs for a specific asset
- `/addresses/{address}/transactions` - Get all transactions for an address

These endpoints focus on granular information specific to a single address.

### Accounts & Address Connections (/accounts/...)
When using stake keys, multiple wallet addresses can be derived from a single account:
- `/accounts/{stake_address}` - Get account-level information
- `/accounts/{stake_address}/addresses` - Get all addresses derived from the stake key

These endpoints are broader in scope and often used when working with stake keys or account-level data.

### When to Use Which
- Use `/addresses/...` endpoints when querying specific wallet addresses
- Use `/accounts/{stake_address}/addresses` or similar plural endpoints when you need to query information for accounts or multiple linked addresses

### Common Confusion Points
- Terminology overlap between "address" and "addresses" in API path names
- Different purposes: singular endpoints work with one address, while plural endpoints can include multiple addresses or addresses linked to an account
