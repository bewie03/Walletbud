"""Utility functions for WalletBud"""

import re
import logging
from typing import Optional, Dict, Any, Tuple
from decimal import Decimal

logger = logging.getLogger(__name__)

def validate_cardano_address(address: str) -> bool:
    """Validate a Cardano address format"""
    from cardano.address_validation import validate_cardano_address as _validate_address
    return _validate_address(address)

def format_ada_amount(lovelace: int) -> str:
    """Format lovelace amount to ADA with proper decimal places"""
    try:
        ada = Decimal(lovelace) / Decimal(1000000)
        return f"{ada:,.6f} ₳"
    except (ValueError, TypeError, ZeroDivisionError) as e:
        logger.error(f"Error formatting ADA amount: {e}")
        return "0.000000 ₳"

def get_asset_info(asset_id: str) -> Tuple[str, str]:
    """Extract policy ID and asset name from asset ID"""
    try:
        if not asset_id or len(asset_id) < 56:
            raise ValueError("Invalid asset ID length")
        
        policy_id = asset_id[:56]
        asset_name = asset_id[56:] if len(asset_id) > 56 else ""
        
        return policy_id, asset_name
    except Exception as e:
        logger.error(f"Error parsing asset ID {asset_id}: {e}")
        return "", ""

def parse_asset_id(asset_id: str) -> Dict[str, str]:
    """Parse an asset ID into its components"""
    policy_id, asset_name = get_asset_info(asset_id)
    return {
        "policy_id": policy_id,
        "asset_name": asset_name,
        "asset_id": asset_id
    }

def format_token_amount(amount: int, decimals: int = 0) -> str:
    """Format token amount with proper decimal places"""
    try:
        if decimals == 0:
            return f"{amount:,}"
        
        factor = Decimal(10 ** decimals)
        formatted = Decimal(amount) / factor
        
        # Remove trailing zeros after decimal point
        return f"{formatted:,.{decimals}f}".rstrip('0').rstrip('.')
    except (ValueError, TypeError, ZeroDivisionError) as e:
        logger.error(f"Error formatting token amount: {e}")
        return "0"

def get_policy_info(metadata: Dict[str, Any], policy_id: str) -> Dict[str, Any]:
    """Extract policy information from metadata"""
    try:
        policy_info = metadata.get("721", {}).get(policy_id, {})
        return {
            "policy_id": policy_id,
            "metadata": policy_info
        }
    except Exception as e:
        logger.error(f"Error getting policy info: {e}")
        return {"policy_id": policy_id, "metadata": {}}

def get_token_info(metadata: Dict[str, Any], policy_id: str, token_name: str) -> Dict[str, Any]:
    """Extract token information from metadata"""
    try:
        policy_info = get_policy_info(metadata, policy_id)
        token_info = policy_info["metadata"].get(token_name, {})
        return {
            "policy_id": policy_id,
            "token_name": token_name,
            "metadata": token_info
        }
    except Exception as e:
        logger.error(f"Error getting token info: {e}")
        return {"policy_id": policy_id, "token_name": token_name, "metadata": {}}

def validate_policy_id(policy_id: str) -> bool:
    """Validate a Cardano policy ID format"""
    if not policy_id:
        return False
    
    # Policy ID should be 56 characters of hex
    policy_pattern = re.compile(r"^[0-9a-fA-F]{56}$")
    return bool(policy_pattern.match(policy_id))

def validate_token_name(token_name: str) -> bool:
    """Validate a token name format"""
    if not token_name:
        return True  # Empty token name is valid
        
    # Token name should be hex encoded and reasonable length
    token_pattern = re.compile(r"^[0-9a-fA-F]{0,64}$")
    return bool(token_pattern.match(token_name))
