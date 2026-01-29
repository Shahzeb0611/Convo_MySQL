"""
HYBRIDJOIN Algorithm Implementation
=====================================
A stream-based join algorithm for near-real-time data warehousing.
Joins streaming transactional data with disk-based master data (Customer & Product).

Components:
- Stream Buffer: Temporarily holds incoming transactional tuples
- Hash Table: 10,000 slots multi-map storing stream tuples
- Queue: Doubly-linked list for FIFO processing
- Disk Buffer: Holds partition of 500 tuples from master data

Author: Shahzeb
Course: DS3003 & DS3004 - Data Warehousing & Business Intelligence
"""

import threading
import time
import csv
import mysql.connector
from mysql.connector import Error
from collections import defaultdict
from typing import Optional, Dict, List, Any, Tuple
import queue
import sys

# =====================================================
# CONFIGURATION CONSTANTS
# =====================================================
HASH_TABLE_SLOTS = 10000      # hS - Number of slots in hash table
DISK_PARTITION_SIZE = 500     # vP - Size of each disk partition
STREAM_BATCH_SIZE = 100       # Tuples to read from CSV at a time
STREAM_DELAY = 0.01           # Delay between stream batches (simulates real-time)


# =====================================================
# DATA STRUCTURES
# =====================================================

class QueueNode:
    """Node for doubly-linked list queue"""
    def __init__(self, key: Any, data: Dict):
        self.key = key              # Join attribute value (e.g., Customer_ID)
        self.data = data            # Full tuple data
        self.prev: Optional['QueueNode'] = None
        self.next: Optional['QueueNode'] = None


class DoublyLinkedQueue:
    """
    Doubly-linked list queue for FIFO processing.
    Supports O(1) insertion at tail and deletion at any position.
    """
    def __init__(self):
        self.head: Optional[QueueNode] = None
        self.tail: Optional[QueueNode] = None
        self.size = 0
        self.lock = threading.Lock()
    
    def enqueue(self, key: Any, data: Dict) -> QueueNode:
        """Add node to tail of queue"""
        with self.lock:
            node = QueueNode(key, data)
            if self.tail is None:
                self.head = self.tail = node
            else:
                node.prev = self.tail
                self.tail.next = node
                self.tail = node
            self.size += 1
            return node
    
    def dequeue(self) -> Optional[QueueNode]:
        """Remove and return node from head of queue"""
        with self.lock:
            if self.head is None:
                return None
            node = self.head
            self.head = node.next
            if self.head:
                self.head.prev = None
            else:
                self.tail = None
            self.size -= 1
            return node
    
    def remove_node(self, node: QueueNode):
        """Remove specific node from queue (O(1) with direct reference)"""
        with self.lock:
            if node.prev:
                node.prev.next = node.next
            else:
                self.head = node.next
            
            if node.next:
                node.next.prev = node.prev
            else:
                self.tail = node.prev
            
            self.size -= 1
    
    def peek_oldest_key(self) -> Optional[Any]:
        """Return the oldest key without removing"""
        with self.lock:
            return self.head.key if self.head else None
    
    def is_empty(self) -> bool:
        return self.size == 0
    
    def __len__(self) -> int:
        return self.size


class HashTable:
    """
    Multi-map hash table with fixed number of slots.
    Each slot can hold multiple entries (for handling collisions and duplicates).
    """
    def __init__(self, num_slots: int = HASH_TABLE_SLOTS):
        self.num_slots = num_slots
        self.slots: List[List[Tuple[Any, Dict, QueueNode]]] = [[] for _ in range(num_slots)]
        self.total_entries = 0
        self.lock = threading.Lock()
    
    def _hash(self, key: Any) -> int:
        """Hash function to map key to slot"""
        return hash(key) % self.num_slots
    
    def insert(self, key: Any, data: Dict, queue_node: QueueNode) -> bool:
        """Insert tuple into hash table"""
        with self.lock:
            if self.total_entries >= self.num_slots:
                return False  # Hash table full
            
            slot_idx = self._hash(key)
            self.slots[slot_idx].append((key, data, queue_node))
            self.total_entries += 1
            return True
    
    def lookup(self, key: Any) -> List[Tuple[Dict, QueueNode]]:
        """Find all entries matching the key"""
        with self.lock:
            slot_idx = self._hash(key)
            results = []
            for k, data, queue_node in self.slots[slot_idx]:
                if k == key:
                    results.append((data, queue_node))
            return results
    
    def remove(self, key: Any, queue_node: QueueNode) -> bool:
        """Remove specific entry from hash table"""
        with self.lock:
            slot_idx = self._hash(key)
            for i, (k, data, qn) in enumerate(self.slots[slot_idx]):
                if k == key and qn is queue_node:
                    del self.slots[slot_idx][i]
                    self.total_entries -= 1
                    return True
            return False
    
    def available_slots(self) -> int:
        """Return number of free slots"""
        return self.num_slots - self.total_entries
    
    def is_empty(self) -> bool:
        return self.total_entries == 0


class StreamBuffer:
    """Thread-safe buffer for incoming stream tuples"""
    def __init__(self, max_size: int = 50000):
        self.buffer = queue.Queue(maxsize=max_size)
        self.finished = False
    
    def put(self, tuple_data: Dict):
        """Add tuple to buffer"""
        self.buffer.put(tuple_data)
    
    def get(self, timeout: float = 1.0) -> Optional[Dict]:
        """Get tuple from buffer"""
        try:
            return self.buffer.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_batch(self, count: int) -> List[Dict]:
        """Get up to 'count' tuples from buffer"""
        batch = []
        for _ in range(count):
            try:
                item = self.buffer.get_nowait()
                batch.append(item)
            except queue.Empty:
                break
        return batch
    
    def size(self) -> int:
        return self.buffer.qsize()
    
    def mark_finished(self):
        self.finished = True
    
    def is_finished(self) -> bool:
        return self.finished and self.buffer.empty()


# =====================================================
# MASTER DATA MANAGER (Disk-based Relation R)
# =====================================================

class MasterDataManager:
    """
    Manages disk-based master data (Customer & Product).
    Provides indexed access for partition loading.
    """
    def __init__(self, customer_file: str, product_file: str):
        self.customer_data: Dict[int, Dict] = {}  # Indexed by Customer_ID
        self.product_data: Dict[str, Dict] = {}   # Indexed by Product_ID
        self.customer_ids: List[int] = []
        self.product_ids: List[str] = []
        
        self._load_customer_data(customer_file)
        self._load_product_data(product_file)
        
        print(f"[MasterData] Loaded {len(self.customer_data)} customers")
        print(f"[MasterData] Loaded {len(self.product_data)} products")
    
    def _load_customer_data(self, filepath: str):
        """Load customer master data from CSV"""
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                customer_id = int(row['Customer_ID'])
                self.customer_data[customer_id] = {
                    'Customer_ID': customer_id,
                    'Gender': row['Gender'],
                    'Age': row['Age'],
                    'Occupation': int(row['Occupation']),
                    'City_Category': row['City_Category'],
                    'Stay_Years': row['Stay_In_Current_City_Years'],
                    'Marital_Status': int(row['Marital_Status'])
                }
                self.customer_ids.append(customer_id)
    
    def _load_product_data(self, filepath: str):
        """Load product master data from CSV"""
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_id = row['Product_ID']
                self.product_data[product_id] = {
                    'Product_ID': product_id,
                    'Product_Category': row['Product_Category'],
                    'Price': float(row['price$']),
                    'Store_ID': int(row['storeID']),
                    'Supplier_ID': int(row['supplierID']),
                    'Store_Name': row['storeName'],
                    'Supplier_Name': row['supplierName']
                }
                self.product_ids.append(product_id)
    
    def get_customer(self, customer_id: int) -> Optional[Dict]:
        """Get customer by ID"""
        return self.customer_data.get(customer_id)
    
    def get_product(self, product_id: str) -> Optional[Dict]:
        """Get product by ID"""
        return self.product_data.get(product_id)
    
    def get_customer_partition(self, start_key: int, size: int = DISK_PARTITION_SIZE) -> List[Dict]:
        """Load a partition of customers starting from a key"""
        # Find customers with ID >= start_key
        partition = []
        for cid in sorted(self.customer_ids):
            if cid >= start_key:
                partition.append(self.customer_data[cid])
                if len(partition) >= size:
                    break
        return partition


# =====================================================
# HYBRIDJOIN ALGORITHM
# =====================================================

class HybridJoin:
    """
    HYBRIDJOIN Algorithm Implementation
    
    Joins streaming transactional data (S) with disk-based master data (R).
    Uses hash table, queue, and disk buffer for efficient processing.
    """
    
    def __init__(self, db_config: Dict, master_data: MasterDataManager):
        self.db_config = db_config
        self.master_data = master_data
        
        # Core data structures
        self.hash_table = HashTable(HASH_TABLE_SLOTS)
        self.queue = DoublyLinkedQueue()
        self.stream_buffer = StreamBuffer()
        self.disk_buffer: List[Dict] = []
        
        # Control variables
        self.w = HASH_TABLE_SLOTS  # Available slots
        self.running = False
        self.joined_count = 0
        self.processed_count = 0
        
        # Database connection
        self.db_connection = None
        
        # Statistics
        self.stats = {
            'stream_tuples_received': 0,
            'tuples_joined': 0,
            'tuples_loaded_to_dw': 0,
            'partitions_loaded': 0
        }
    
    def connect_database(self):
        """Connect to MySQL database"""
        try:
            self.db_connection = mysql.connector.connect(**self.db_config)
            print("[HybridJoin] Connected to database successfully")
            return True
        except Error as e:
            print(f"[HybridJoin] Database connection error: {e}")
            return False
    
    def create_dw_table(self):
        """Create the enriched DW table if not exists"""
        cursor = self.db_connection.cursor()
        
        # Create enriched fact table for joined data
        create_sql = """
        CREATE TABLE IF NOT EXISTS DW_ENRICHED_TRANSACTIONS (
            transaction_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT,
            order_date DATE,
            quantity INT,
            -- Customer dimensions
            customer_id INT,
            gender VARCHAR(10),
            age VARCHAR(20),
            occupation INT,
            city_category VARCHAR(10),
            stay_years VARCHAR(10),
            marital_status INT,
            -- Product dimensions
            product_id VARCHAR(20),
            product_category VARCHAR(50),
            price DECIMAL(10,2),
            store_id INT,
            supplier_id INT,
            store_name VARCHAR(100),
            supplier_name VARCHAR(100),
            -- Calculated fields
            total_amount DECIMAL(12,2),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_sql)
        self.db_connection.commit()
        print("[HybridJoin] DW_ENRICHED_TRANSACTIONS table ready")
        cursor.close()
    
    def load_to_dw(self, enriched_tuple: Dict):
        """Load enriched tuple into Data Warehouse"""
        if not self.db_connection:
            return
        
        cursor = self.db_connection.cursor()
        
        insert_sql = """
        INSERT INTO DW_ENRICHED_TRANSACTIONS 
        (order_id, order_date, quantity, customer_id, gender, age, occupation,
         city_category, stay_years, marital_status, product_id, product_category,
         price, store_id, supplier_id, store_name, supplier_name, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            enriched_tuple.get('order_id'),
            enriched_tuple.get('order_date'),
            enriched_tuple.get('quantity'),
            enriched_tuple.get('customer_id'),
            enriched_tuple.get('gender'),
            enriched_tuple.get('age'),
            enriched_tuple.get('occupation'),
            enriched_tuple.get('city_category'),
            enriched_tuple.get('stay_years'),
            enriched_tuple.get('marital_status'),
            enriched_tuple.get('product_id'),
            enriched_tuple.get('product_category'),
            enriched_tuple.get('price'),
            enriched_tuple.get('store_id'),
            enriched_tuple.get('supplier_id'),
            enriched_tuple.get('store_name'),
            enriched_tuple.get('supplier_name'),
            enriched_tuple.get('total_amount')
        )
        
        try:
            cursor.execute(insert_sql, values)
            self.stats['tuples_loaded_to_dw'] += 1
        except Error as e:
            pass  # Skip duplicates or errors silently
        finally:
            cursor.close()
    
    def stream_producer(self, transaction_file: str):
        """
        THREAD 1: Stream Producer
        Continuously reads transactional data from CSV and feeds into stream buffer.
        Simulates near-real-time data arrival.
        """
        print(f"[StreamProducer] Starting to stream from {transaction_file}")
        
        with open(transaction_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            batch = []
            
            for row in reader:
                if not self.running:
                    break
                
                # Parse transaction tuple
                tuple_data = {
                    'order_id': int(row['orderID']),
                    'customer_id': int(row['Customer_ID']),
                    'product_id': row['Product_ID'],
                    'quantity': int(row['quantity']),
                    'order_date': row['date']
                }
                
                self.stream_buffer.put(tuple_data)
                self.stats['stream_tuples_received'] += 1
                
                # Simulate real-time streaming with small delay
                if self.stats['stream_tuples_received'] % STREAM_BATCH_SIZE == 0:
                    time.sleep(STREAM_DELAY)
                    
                    # Progress update
                    if self.stats['stream_tuples_received'] % 10000 == 0:
                        print(f"[StreamProducer] Streamed {self.stats['stream_tuples_received']} tuples...")
        
        self.stream_buffer.mark_finished()
        print(f"[StreamProducer] Finished streaming {self.stats['stream_tuples_received']} tuples")
    
    def join_consumer(self):
        """
        THREAD 2: HYBRIDJOIN Consumer
        Implements the HYBRIDJOIN algorithm to join stream with master data.
        """
        print("[JoinConsumer] Starting HYBRIDJOIN algorithm...")
        
        iteration = 0
        commit_batch = 0
        
        while self.running or not self.stream_buffer.is_finished() or not self.hash_table.is_empty():
            iteration += 1
            
            # =====================================================
            # STEP 1: Load stream tuples into hash table
            # =====================================================
            # Get up to 'w' tuples from stream buffer
            tuples_to_load = min(self.w, self.stream_buffer.size())
            
            if tuples_to_load > 0:
                stream_tuples = self.stream_buffer.get_batch(tuples_to_load)
                
                for tuple_data in stream_tuples:
                    # Use Customer_ID as join key
                    join_key = tuple_data['customer_id']
                    
                    # Add to queue (FIFO order)
                    queue_node = self.queue.enqueue(join_key, tuple_data)
                    
                    # Add to hash table with reference to queue node
                    self.hash_table.insert(join_key, tuple_data, queue_node)
                    
                    self.w -= 1
            
            # =====================================================
            # STEP 2: Get oldest key and load disk partition
            # =====================================================
            oldest_key = self.queue.peek_oldest_key()
            
            if oldest_key is None:
                if self.stream_buffer.is_finished():
                    break
                time.sleep(0.01)  # Wait for more data
                continue
            
            # Load partition from master data (disk) into disk buffer
            self.disk_buffer = self.master_data.get_customer_partition(oldest_key, DISK_PARTITION_SIZE)
            self.stats['partitions_loaded'] += 1
            
            # =====================================================
            # STEP 3: Probe hash table with disk buffer tuples
            # =====================================================
            for customer_data in self.disk_buffer:
                customer_id = customer_data['Customer_ID']
                
                # Look up matching stream tuples in hash table
                matches = self.hash_table.lookup(customer_id)
                
                for stream_tuple, queue_node in matches:
                    # =====================================================
                    # STEP 4: Generate join output (enriched tuple)
                    # =====================================================
                    product_id = stream_tuple['product_id']
                    product_data = self.master_data.get_product(product_id)
                    
                    if product_data:
                        # Create enriched tuple by joining all data
                        enriched_tuple = {
                            # Transaction data
                            'order_id': stream_tuple['order_id'],
                            'order_date': stream_tuple['order_date'],
                            'quantity': stream_tuple['quantity'],
                            # Customer data (enrichment)
                            'customer_id': customer_id,
                            'gender': customer_data['Gender'],
                            'age': customer_data['Age'],
                            'occupation': customer_data['Occupation'],
                            'city_category': customer_data['City_Category'],
                            'stay_years': customer_data['Stay_Years'],
                            'marital_status': customer_data['Marital_Status'],
                            # Product data (enrichment)
                            'product_id': product_id,
                            'product_category': product_data['Product_Category'],
                            'price': product_data['Price'],
                            'store_id': product_data['Store_ID'],
                            'supplier_id': product_data['Supplier_ID'],
                            'store_name': product_data['Store_Name'],
                            'supplier_name': product_data['Supplier_Name'],
                            # Calculated
                            'total_amount': stream_tuple['quantity'] * product_data['Price']
                        }
                        
                        # Load enriched tuple into DW
                        self.load_to_dw(enriched_tuple)
                        self.stats['tuples_joined'] += 1
                    
                    # =====================================================
                    # STEP 5: Remove matched tuple from hash table and queue
                    # =====================================================
                    self.hash_table.remove(customer_id, queue_node)
                    self.queue.remove_node(queue_node)
                    
                    # Free up slot
                    self.w += 1
            
            # Commit periodically
            commit_batch += 1
            if commit_batch >= 100:
                if self.db_connection:
                    self.db_connection.commit()
                commit_batch = 0
            
            # Progress update
            if iteration % 100 == 0:
                print(f"[JoinConsumer] Iteration {iteration}: Joined={self.stats['tuples_joined']}, "
                      f"Queue={len(self.queue)}, HashTable={self.hash_table.total_entries}")
        
        # Final commit
        if self.db_connection:
            self.db_connection.commit()
        
        print(f"[JoinConsumer] HYBRIDJOIN completed!")
        print(f"[JoinConsumer] Total joined: {self.stats['tuples_joined']}")
    
    def run(self, transaction_file: str):
        """
        Main execution method.
        Starts both threads and coordinates the join operation.
        """
        print("\n" + "=" * 70)
        print("HYBRIDJOIN ALGORITHM - Near Real-Time Data Warehouse")
        print("=" * 70)
        
        # Connect to database
        if not self.connect_database():
            print("Failed to connect to database. Exiting.")
            return
        
        # Create DW table
        self.create_dw_table()
        
        self.running = True
        
        # Create threads
        producer_thread = threading.Thread(
            target=self.stream_producer,
            args=(transaction_file,),
            name="StreamProducer"
        )
        
        consumer_thread = threading.Thread(
            target=self.join_consumer,
            name="JoinConsumer"
        )
        
        # Start threads
        print("\n[Main] Starting threads...")
        start_time = time.time()
        
        producer_thread.start()
        time.sleep(0.5)  # Let producer get a head start
        consumer_thread.start()
        
        # Wait for completion
        producer_thread.join()
        self.running = False
        consumer_thread.join()
        
        end_time = time.time()
        
        # Print final statistics
        print("\n" + "=" * 70)
        print("HYBRIDJOIN EXECUTION STATISTICS")
        print("=" * 70)
        print(f"  Stream tuples received:    {self.stats['stream_tuples_received']:,}")
        print(f"  Tuples successfully joined: {self.stats['tuples_joined']:,}")
        print(f"  Tuples loaded to DW:        {self.stats['tuples_loaded_to_dw']:,}")
        print(f"  Disk partitions loaded:     {self.stats['partitions_loaded']:,}")
        print(f"  Execution time:             {end_time - start_time:.2f} seconds")
        print("=" * 70)
        
        # Close database connection
        if self.db_connection:
            self.db_connection.close()
            print("[Main] Database connection closed")


# =====================================================
# MAIN EXECUTION
# =====================================================

def get_database_credentials():
    """Get database credentials from user"""
    print("\n" + "=" * 70)
    print("HYBRIDJOIN - Database Configuration")
    print("=" * 70)
    
    host = input("Enter MySQL host [localhost]: ").strip() or "localhost"
    port = input("Enter MySQL port [3306]: ").strip() or "3306"
    user = input("Enter MySQL username [root]: ").strip() or "root"
    password = input("Enter MySQL password: ").strip()
    database = input("Enter database name [project_test]: ").strip() or "project_test"
    
    return {
        'host': host,
        'port': int(port),
        'user': user,
        'password': password,
        'database': database
    }


def main():
    """Main function"""
    print("\n" + "=" * 70)
    print("   HYBRIDJOIN ALGORITHM IMPLEMENTATION")
    print("   Near Real-Time Data Warehouse Loading")
    print("=" * 70)
    print("\nThis program implements the HYBRIDJOIN algorithm to:")
    print("  1. Stream transactional data from CSV")
    print("  2. Join with customer and product master data")
    print("  3. Load enriched data into the Data Warehouse")
    print("\nData Structures used:")
    print(f"  - Hash Table: {HASH_TABLE_SLOTS:,} slots")
    print(f"  - Disk Partition Size: {DISK_PARTITION_SIZE} tuples")
    print("  - Queue: Doubly-linked list (FIFO)")
    print("=" * 70)
    
    # Get database credentials from user
    db_config = get_database_credentials()
    
    # File paths
    import os
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
    
    # Load master data (disk-based relation R)
    master_data = MasterDataManager(customer_file, product_file)
    
    # Create and run HYBRIDJOIN
    hybrid_join = HybridJoin(db_config, master_data)
    hybrid_join.run(transaction_file)
    
    print("\n[Main] HYBRIDJOIN execution completed successfully!")
    print("Check the DW_ENRICHED_TRANSACTIONS table for results.")


if __name__ == "__main__":
    main()
