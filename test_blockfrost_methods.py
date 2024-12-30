from blockfrost import BlockFrostApi
import inspect

def list_address_methods():
    """List all address-related methods in BlockFrostApi"""
    client = BlockFrostApi(
        project_id="dummy",  # Won't actually connect
        base_url="https://cardano-mainnet.blockfrost.io/api/v0"
    )
    
    # Get all methods
    methods = inspect.getmembers(client, predicate=inspect.ismethod)
    
    # Filter for address methods and print their signatures
    address_methods = [m for m in methods if 'address' in m[0]]
    for name, method in address_methods:
        print(f"\nMethod: {name}")
        print(f"Signature: {inspect.signature(method)}")

if __name__ == "__main__":
    list_address_methods()
