"""
Quick runner for HYBRIDJOIN with preset credentials
"""
import os
import sys

# Add the current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the hybrid join module
from hybrid_join import HybridJoin, MasterDataManager, HASH_TABLE_SLOTS, DISK_PARTITION_SIZE

def main():
    print("\n" + "=" * 70)
    print("   HYBRIDJOIN ALGORITHM - QUICK RUN")
    print("=" * 70)
    
    # Preset credentials
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '1234',
        'database': 'project_test'
    }
    
    print(f"\nUsing database: {db_config['database']} @ {db_config['host']}")
    print(f"Hash Table Slots: {HASH_TABLE_SLOTS:,}")
    print(f"Disk Partition Size: {DISK_PARTITION_SIZE}")
    
    # File paths
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(base_path, 'data')
    
    customer_file = os.path.join(data_folder, 'customer_master_data.csv')
    product_file = os.path.join(data_folder, 'product_master_data.csv')
    transaction_file = os.path.join(data_folder, 'transactional_data.csv')
    
    # Verify files exist
    for f in [customer_file, product_file, transaction_file]:
        if not os.path.exists(f):
            print(f"Error: File not found: {f}")
            return
    
    print("\n[Main] Loading master data...")
    
    # Load master data
    master_data = MasterDataManager(customer_file, product_file)
    
    # Create and run HYBRIDJOIN
    hybrid_join = HybridJoin(db_config, master_data)
    hybrid_join.run(transaction_file)
    
    print("\n[Main] HYBRIDJOIN execution completed!")

if __name__ == "__main__":
    main()
