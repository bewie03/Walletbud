"""Cardano address validation utilities"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Cardano address format patterns
MAINNET_PATTERNS = {
    'base': r'^addr1[a-zA-Z0-9]{98}$',  # Base addresses
    'enterprise': r'^addr1v[a-zA-Z0-9]{97}$',  # Enterprise addresses
    'pointer': r'^addr1g[a-zA-Z0-9]{97}$',  # Pointer addresses
    'stake': r'^stake1[a-zA-Z0-9]{53}$',  # Stake addresses
}

def validate_cardano_address(address: str) -> bool:
    """Validate a Cardano address format
    
    Args:
        address: The address to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not address:
        logger.warning("Empty address provided")
        return False
        
    try:
        # Check against each pattern
        for addr_type, pattern in MAINNET_PATTERNS.items():
            if re.match(pattern, address):
                logger.debug(f"Address matches {addr_type} pattern")
                return True
                
        logger.warning(f"Address does not match any known pattern: {address[:10]}...")
        return False
        
    except Exception as e:
        logger.error(f"Error validating address: {e}")
        return False
