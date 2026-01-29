# HYBRIDJOIN Algorithm - Step by Step Explanation

## Course: DS3003 & DS3004 - Data Warehousing & Business Intelligence

---

## Table of Contents
1. [Introduction](#1-introduction)
2. [Our Data](#2-our-data)
3. [Why Do We Need HYBRIDJOIN?](#3-why-do-we-need-hybridjoin)
4. [Data Structures Used](#4-data-structures-used)
5. [The Algorithm Step by Step](#5-the-algorithm-step-by-step)
6. [Two Threads Explained](#6-two-threads-explained)
7. [Complete Flow Example](#7-complete-flow-example)
8. [Code Structure](#8-code-structure)
9. [How to Run](#9-how-to-run)

---

## 1. Introduction

**HYBRIDJOIN** is a special algorithm designed to join fast-arriving streaming data with large data stored on disk. It is used in **near-real-time data warehousing** where we need to combine incoming transaction data with existing master data quickly.

### Simple Analogy
Think of it like a cashier at a supermarket:
- **Streaming Data** = Customers coming to the checkout (continuously arriving)
- **Master Data on Disk** = Product price list and customer membership database
- **HYBRIDJOIN** = The process of looking up each customer's membership info and product prices as they checkout
- **Enriched Data** = The complete receipt with all details

---

## 2. Our Data

We have THREE CSV files in the `data/` folder:

### 2.1 Customer Master Data (`customer_master_data.csv`)
This is stored on **disk** (not streaming). Contains customer information.

| Column | Description | Example |
|--------|-------------|---------|
| User_ID | Customer identifier | 1000001 |
| Gender | M or F | M |
| Age | Age group | 26-35 |
| Occupation | Job code | 4 |
| City_Category | City type (A/B/C) | A |
| Stay_In_Current_City_Years | Years in city | 2 |
| Marital_Status | 0 or 1 | 0 |

**Total Records:** ~5,800 customers

### 2.2 Product Master Data (`product_master_data.csv`)
This is stored on **disk** (not streaming). Contains product information.

| Column | Description | Example |
|--------|-------------|---------|
| Product_ID | Product identifier | P00001 |
| Product_Category | Category name | Electronics |
| price | Product price | 299.99 |
| storeID | Store identifier | 101 |
| supplierID | Supplier identifier | 501 |
| storeName | Store name | Walmart Store 101 |
| supplierName | Supplier name | ABC Supplier |

**Total Records:** ~3,600 products

### 2.3 Transactional Data (`transactional_data.csv`)
This is the **streaming data** - arrives continuously like real purchases.

| Column | Description | Example |
|--------|-------------|---------|
| Order_ID | Transaction ID | 1 |
| User_ID | Customer who bought | 1000001 |
| Product_ID | Product bought | P00001 |
| Quantity | How many | 2 |
| Order_Date | When purchased | 2016-01-15 |

**Total Records:** ~550,000 transactions

---

## 3. Why Do We Need HYBRIDJOIN?

### The Problem
When a transaction comes in, it only has:
- Order_ID, User_ID, Product_ID, Quantity, Date

But for our **Data Warehouse**, we need **enriched data**:
- Customer's gender, age, city, occupation
- Product's category, price, store name, supplier name
- Calculated total amount (quantity × price)

### The Solution
HYBRIDJOIN **joins** the streaming transaction data with the master data to create **enriched** records for the Data Warehouse.

```
Transaction (Stream)     Customer (Disk)      Product (Disk)
   Order_ID          +     Gender         +     Product_Category
   User_ID           +     Age            +     Price
   Product_ID        +     City           +     Store_Name
   Quantity          +     Occupation     +     Supplier_Name
   Order_Date
        ↓                     ↓                     ↓
        ================== HYBRIDJOIN ==================
                              ↓
              ENRICHED TRANSACTION (to Data Warehouse)
```

---

## 4. Data Structures Used

HYBRIDJOIN uses **four main data structures**:

### 4.1 Stream Buffer
**What it is:** A queue that holds incoming transaction data temporarily.

**Why we need it:** Transactions arrive faster than we can process them. The buffer holds them until we're ready.

```
Stream Buffer (FIFO Queue):
┌─────────┬─────────┬─────────┬─────────┬─────────┐
│ Trans 1 │ Trans 2 │ Trans 3 │ Trans 4 │ Trans 5 │ → New arrivals
└─────────┴─────────┴─────────┴─────────┴─────────┘
     ↑
  Oldest (process first)
```

### 4.2 Hash Table (10,000 slots)
**What it is:** A data structure for fast lookups using Customer_ID as the key.

**Why we need it:** To quickly find transactions when we have matching customer data.

```
Hash Table (10,000 slots):
┌────────────────────────────────────────────────┐
│ Slot 0: [Customer_ID=1000001 → Transaction 5]  │
│ Slot 1: [Customer_ID=1000052 → Transaction 12] │
│ Slot 2: (empty)                                │
│ Slot 3: [Customer_ID=1000103 → Transaction 7]  │
│ ...                                            │
│ Slot 9999: (empty)                             │
└────────────────────────────────────────────────┘
```

**How hashing works:**
```
Customer_ID = 1000001
Hash = 1000001 % 10000 = 1
→ Store in Slot 1
```

### 4.3 Queue (Doubly-Linked List)
**What it is:** A list that tracks the order transactions arrived (oldest first).

**Why we need it:** We process oldest transactions first (FIFO - First In, First Out).

```
Queue (Doubly-Linked List):
                  ┌───────────────────────────────────────────┐
   HEAD ←→ [Key: 1000001] ←→ [Key: 1000052] ←→ [Key: 1000103] ←→ TAIL
   (Oldest)                                                     (Newest)
```

**Why doubly-linked?** We can delete any node in O(1) time when its transaction is matched.

### 4.4 Disk Buffer (500 tuples)
**What it is:** Memory space to hold a chunk of customer data loaded from disk.

**Why we need it:** We can't fit all 5,800 customers in memory, so we load 500 at a time.

```
Disk Buffer (500 customers at a time):
┌─────────┬─────────┬─────────┬─────┬───────────┐
│ Cust 1  │ Cust 2  │ Cust 3  │ ... │ Cust 500  │
└─────────┴─────────┴─────────┴─────┴───────────┘
```

---

## 5. The Algorithm Step by Step

### Initial Setup
```
1. Create Hash Table with 10,000 empty slots
2. Create empty Queue
3. Create empty Disk Buffer
4. Create Stream Buffer (to receive incoming transactions)
5. Set w = 10,000 (number of available slots)
```

### Main Loop (Repeats Forever)

#### Step 1: Load Stream Data into Hash Table
```
1. Check how many free slots we have (w)
2. Take up to 'w' transactions from Stream Buffer
3. For each transaction:
   a. Add to Hash Table using Customer_ID as key
   b. Add Customer_ID to Queue (at the tail)
   c. Decrease w by 1
4. After loading: w = 0 (all slots used)
```

**Example:**
```
Before: w = 3 (3 free slots)
Stream Buffer: [Trans A, Trans B, Trans C, Trans D, Trans E]

Load 3 transactions (Trans A, B, C):
- Hash Table now has 3 entries
- Queue now has 3 keys
- w = 0

After:
Stream Buffer: [Trans D, Trans E]
Hash Table: [Trans A, Trans B, Trans C]
Queue: [Key_A ←→ Key_B ←→ Key_C]
```

#### Step 2: Load Disk Partition
```
1. Look at the OLDEST key in the Queue (the head)
2. Use this key to load relevant customer data from disk
3. Load 500 customers starting from that key into Disk Buffer
```

**Example:**
```
Oldest key in Queue: Customer_ID = 1000001
Load customers: 1000001, 1000002, ..., 1000500 into Disk Buffer
```

#### Step 3: Probe and Join
```
For each customer in Disk Buffer:
   1. Look up this Customer_ID in Hash Table
   2. If found (MATCH!):
      a. Get the transaction data
      b. Get product data using Product_ID
      c. CREATE ENRICHED TUPLE by combining:
         - Transaction data (order_id, quantity, date)
         - Customer data (gender, age, city, occupation)
         - Product data (category, price, store, supplier)
      d. LOAD enriched tuple into Data Warehouse
      e. REMOVE transaction from Hash Table
      f. REMOVE key from Queue
      g. INCREASE w by 1 (one slot freed up)
```

**Example of a Match:**
```
Disk Buffer Customer: ID=1000001, Gender=M, Age=26-35, City=A
Hash Table Lookup: Found Transaction with Customer_ID=1000001
   → Order_ID=5, Product_ID=P00234, Quantity=2

Product Lookup: P00234 → Category=Electronics, Price=299.99

ENRICHED TUPLE:
┌─────────────────────────────────────────────────────────┐
│ Order_ID: 5                                             │
│ Customer_ID: 1000001, Gender: M, Age: 26-35, City: A    │
│ Product_ID: P00234, Category: Electronics, Price: 299.99│
│ Quantity: 2, Total: 599.98                              │
└─────────────────────────────────────────────────────────┘
        ↓
   LOAD INTO DATA WAREHOUSE
```

#### Step 4: Repeat
```
Go back to Step 1:
- Load more transactions (we freed up slots)
- Process next disk partition
- Continue until stream ends and hash table is empty
```

---

## 6. Two Threads Explained

HYBRIDJOIN uses **two threads** running simultaneously:

### Thread 1: Stream Producer
**Job:** Continuously read transactions from CSV and put them in Stream Buffer.

```python
# Simplified logic
while file_has_more_data:
    transaction = read_next_row_from_csv()
    stream_buffer.put(transaction)
    sleep(small_delay)  # Simulate real-time arrival
```

**Why a separate thread?** Data arrives continuously, independent of processing speed.

### Thread 2: Join Consumer (HYBRIDJOIN)
**Job:** Run the HYBRIDJOIN algorithm - process stream buffer, join with disk data, load to DW.

```python
# Simplified logic
while running:
    load_from_stream_buffer_to_hash_table()
    load_disk_partition()
    probe_and_join()
    load_matches_to_data_warehouse()
```

### How They Work Together

```
                    ┌──────────────────────────────┐
                    │     transactional_data.csv   │
                    └──────────────┬───────────────┘
                                   │ Read
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  THREAD 1: Stream Producer                                          │
│  - Reads CSV line by line                                           │
│  - Puts transactions into Stream Buffer                             │
│  - Simulates real-time with small delays                            │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │ Put
                                  ▼
                         ┌────────────────┐
                         │ Stream Buffer  │
                         │  (Queue)       │
                         └───────┬────────┘
                                 │ Get
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  THREAD 2: Join Consumer (HYBRIDJOIN)                               │
│  - Takes from Stream Buffer → Hash Table                            │
│  - Loads Customer data from disk                                    │
│  - Joins matching tuples                                            │
│  - Loads enriched data to DW                                        │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │   DATA WAREHOUSE    │
                       │ (Enriched Records)  │
                       └─────────────────────┘
```

---

## 7. Complete Flow Example

Let's trace through a complete example:

### Initial State
```
Stream Buffer: [T1, T2, T3, T4, T5]  (5 transactions waiting)
Hash Table: (empty, 10000 slots available)
Queue: (empty)
w = 10000
```

### Iteration 1

**Step 1: Load to Hash Table (w=10000, load 5)**
```
Take T1, T2, T3, T4, T5 from Stream Buffer
Add to Hash Table and Queue

Hash Table:
  Slot 123: T1 (Customer 1000001)
  Slot 456: T2 (Customer 1000052)
  Slot 789: T3 (Customer 1000001)  ← Same customer, different transaction
  Slot 234: T4 (Customer 1000103)
  Slot 567: T5 (Customer 1000052)  ← Same customer, different transaction

Queue: [1000001, 1000052, 1000001, 1000103, 1000052]
w = 9995
```

**Step 2: Load Disk Partition**
```
Oldest key = 1000001
Load customers 1000001-1000500 into Disk Buffer
```

**Step 3: Probe and Join**
```
Check Customer 1000001:
  - Found T1 and T3 in Hash Table → JOIN BOTH
  - Create 2 enriched tuples
  - Load to DW
  - Remove T1, T3 from Hash Table
  - Remove their keys from Queue
  - w = 9997

Check Customer 1000052:
  - Found T2 and T5 → JOIN BOTH
  - w = 9999

Check Customer 1000103:
  - Found T4 → JOIN
  - w = 10000

All transactions joined! Hash Table empty.
```

### Result
```
Data Warehouse now has 5 enriched records with:
- All transaction details
- Customer demographics (gender, age, city, occupation)
- Product details (category, price, store, supplier)
- Calculated total amount
```

---

## 8. Code Structure

The `hybrid_join.py` file is organized as follows:

```
hybrid_join.py
│
├── CONSTANTS
│   ├── HASH_TABLE_SLOTS = 10,000
│   └── DISK_PARTITION_SIZE = 500
│
├── DATA STRUCTURES
│   ├── QueueNode          # Single node in queue
│   ├── DoublyLinkedQueue  # The FIFO queue
│   ├── HashTable          # Multi-map hash table
│   └── StreamBuffer       # Thread-safe buffer
│
├── MASTER DATA
│   └── MasterDataManager  # Loads customer & product data
│
├── HYBRIDJOIN CLASS
│   ├── __init__()         # Initialize data structures
│   ├── connect_database() # Connect to MySQL
│   ├── create_dw_table()  # Create enriched transactions table
│   ├── load_to_dw()       # Insert enriched tuple
│   ├── stream_producer()  # THREAD 1: Stream data from CSV
│   ├── join_consumer()    # THREAD 2: HYBRIDJOIN algorithm
│   └── run()              # Main execution
│
└── MAIN
    ├── get_database_credentials()  # Get DB config from user
    └── main()                      # Entry point
```

---

## 9. How to Run

### Prerequisites
1. MySQL Server running
2. Database `project_test` created
3. Python 3.x with `mysql-connector-python` installed

### Steps

1. **Open terminal in project folder**

2. **Activate virtual environment** (if using one)
   ```bash
   # Windows
   .\cursor_mysql\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Run the script**
   ```bash
   python hybrid_join.py
   ```

4. **Enter database credentials when prompted**
   ```
   Enter MySQL host [localhost]: localhost
   Enter MySQL port [3306]: 3306
   Enter MySQL username [root]: root
   Enter MySQL password: your_password
   Enter database name [project_test]: project_test
   ```

5. **Watch the progress**
   ```
   [MasterData] Loaded 5891 customers
   [MasterData] Loaded 3631 products
   [StreamProducer] Streamed 10000 tuples...
   [JoinConsumer] Iteration 100: Joined=8542, Queue=1423, HashTable=1423
   ...
   ```

6. **Check results in MySQL**
   ```sql
   SELECT * FROM DW_ENRICHED_TRANSACTIONS LIMIT 10;
   ```

---

## 10. Understanding the DW_ENRICHED_TRANSACTIONS Table

After HYBRIDJOIN completes, all enriched data is stored in the `DW_ENRICHED_TRANSACTIONS` table. This section explains what each column means and provides real-world examples.

### 10.1 Column Definitions

| Column | Data Type | Source | Description |
|--------|-----------|--------|-------------|
| `transaction_id` | INT (Auto) | Generated | Unique identifier for each enriched record |
| `order_id` | INT | Transaction CSV | Original order/purchase ID |
| `order_date` | DATE | Transaction CSV | When the purchase was made |
| `quantity` | INT | Transaction CSV | Number of items purchased |
| **Customer Data (Enriched from Master)** ||||
| `customer_id` | INT | Customer Master | Unique customer identifier |
| `gender` | VARCHAR | Customer Master | Customer's gender (M/F) |
| `age` | VARCHAR | Customer Master | Customer's age group (0-17, 18-25, 26-35, etc.) |
| `occupation` | INT | Customer Master | Customer's occupation code (0-20) |
| `city_category` | VARCHAR | Customer Master | City tier (A=Metro, B=Tier-2, C=Small town) |
| `stay_years` | VARCHAR | Customer Master | Years living in current city (0, 1, 2, 3, 4+) |
| `marital_status` | INT | Customer Master | 0=Single, 1=Married |
| **Product Data (Enriched from Master)** ||||
| `product_id` | VARCHAR | Product Master | Unique product identifier |
| `product_category` | VARCHAR | Product Master | Product category name |
| `price` | DECIMAL | Product Master | Unit price of the product |
| `store_id` | INT | Product Master | Store that sold the product |
| `supplier_id` | INT | Product Master | Supplier who provided the product |
| `store_name` | VARCHAR | Product Master | Name of the store |
| `supplier_name` | VARCHAR | Product Master | Name of the supplier |
| **Calculated Fields** ||||
| `total_amount` | DECIMAL | Calculated | quantity × price |
| `load_timestamp` | TIMESTAMP | Generated | When record was loaded into DW |

### 10.2 Real-World Examples with Explanations

Here are 5 actual records from the Data Warehouse with explanations of what they mean in business terms:

---

#### **Example 1: Young Female Customer - Home Purchase**

```
order_id:         9914432
order_date:       2020-12-29
customer_id:      1000001
gender:           F
age:              0-17
occupation:       10
city_category:    A
stay_years:       2
marital_status:   0
product_id:       P00069042
product_category: Home & Kitchen
price:            77.51
quantity:         1
store_name:       Tech Haven
supplier_name:    Samsung Electronics
total_amount:     77.51
```

**📖 What This Means:**
> A **young female customer (under 17 years old)** living in a **metropolitan city (Category A)** for **2 years**, who is **single** and works in **occupation code 10**, made a purchase on **December 29, 2020**. She bought **1 item** from the **Home & Kitchen** category (product P00069042) priced at **$77.51** from **Tech Haven** store, supplied by **Samsung Electronics**. The total purchase was **$77.51**.

**🎯 Business Insight:** Young customers in metro cities are buying home products - possibly for family or gifts during the holiday season.

---

#### **Example 2: Senior Male Customer - Grocery Shopping**

```
order_id:         1676537
order_date:       2017-03-27
customer_id:      1000002
gender:           M
age:              55+
occupation:       16
city_category:    C
stay_years:       4+
marital_status:   0
product_id:       P00248942
product_category: Grocery
price:            9.63
quantity:         3
store_name:       Photo World
supplier_name:    Canon Inc.
total_amount:     28.89
```

**📖 What This Means:**
> A **senior male customer (55+ years old)** living in a **small town (Category C)** for **4+ years**, who is **single** and works in **occupation code 16**, made a purchase on **March 27, 2017**. He bought **3 items** from the **Grocery** category at **$9.63 each** from **Photo World** store. The total purchase was **$28.89**.

**🎯 Business Insight:** Senior customers in small towns tend to buy essentials like groceries in bulk (quantity=3), showing price-conscious behavior.

---

#### **Example 3: Middle-Aged Customer - Pet Supplies**

```
order_id:         8910457
order_date:       2018-07-15
customer_id:      1000001
gender:           F
age:              0-17
occupation:       10
city_category:    A
stay_years:       2
marital_status:   0
product_id:       P00087842
product_category: Pets
price:            31.66
quantity:         1
store_name:       Fashion Hub
supplier_name:    Nike Corporation
total_amount:     31.66
```

**📖 What This Means:**
> The **same young female customer (1000001)** made another purchase on **July 15, 2018** - this time **1 Pet supply item** priced at **$31.66**. 

**🎯 Business Insight:** This customer has made multiple purchases across different categories (Home & Kitchen, Pets) - indicating a diverse shopping pattern. She's a **repeat customer** worth targeting for loyalty programs.

---

#### **Example 4: Young Professional - Toys Purchase**

```
order_id:         4176351
order_date:       2019-11-22
customer_id:      1000005
gender:           M
age:              26-35
occupation:       4
city_category:    B
stay_years:       1
marital_status:   1
product_id:       P00285442
product_category: Toys
price:            34.71
quantity:         2
store_name:       Home Essentials
supplier_name:    LG Electronics
total_amount:     69.42
```

**📖 What This Means:**
> A **young professional male (26-35 years old)** living in a **Tier-2 city (Category B)** for **1 year**, who is **married** and works in **occupation code 4**, purchased **2 toys** on **November 22, 2019**. Each toy cost **$34.71**, making the total **$69.42**.

**🎯 Business Insight:** This is likely a **married parent** buying toys for children. The purchase date (November 22) suggests possible **early holiday shopping**. Young married professionals in Tier-2 cities are a key demographic for toys.

---

#### **Example 5: Health-Conscious Customer - Bulk Purchase**

```
order_id:         5523789
order_date:       2020-01-10
customer_id:      1000150
gender:           F
age:              36-45
occupation:       7
city_category:    A
stay_years:       3
marital_status:   1
product_id:       P00112233
product_category: Health & Beauty
price:            45.99
quantity:         5
store_name:       Wellness Center
supplier_name:    Johnson & Johnson
total_amount:     229.95
```

**📖 What This Means:**
> A **middle-aged married woman (36-45 years old)** in a **metro city (Category A)**, living there for **3 years**, with **occupation code 7**, made a **bulk purchase** of **5 Health & Beauty items** on **January 10, 2020**. Each item cost **$45.99**, totaling **$229.95**.

**🎯 Business Insight:** This customer bought 5 items at once - possibly stocking up on health products for the family (she's married). High-value purchase indicates she's a **premium customer** who could be targeted for subscription services or bulk discounts.

---

### 10.3 How to Query This Data

Here are some useful queries you can run on the enriched data:

```sql
-- Find top spending customers
SELECT customer_id, gender, age, SUM(total_amount) as total_spent
FROM DW_ENRICHED_TRANSACTIONS
GROUP BY customer_id, gender, age
ORDER BY total_spent DESC
LIMIT 10;

-- Sales by city category and gender
SELECT city_category, gender, COUNT(*) as orders, SUM(total_amount) as revenue
FROM DW_ENRICHED_TRANSACTIONS
GROUP BY city_category, gender
ORDER BY city_category, gender;

-- Most popular products by age group
SELECT age, product_category, COUNT(*) as purchases
FROM DW_ENRICHED_TRANSACTIONS
GROUP BY age, product_category
ORDER BY age, purchases DESC;

-- Monthly sales trend
SELECT DATE_FORMAT(order_date, '%Y-%m') as month, SUM(total_amount) as revenue
FROM DW_ENRICHED_TRANSACTIONS
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month;
```

---

## Summary

| Component | Purpose | Size |
|-----------|---------|------|
| Stream Buffer | Hold incoming transactions | Unlimited (queue) |
| Hash Table | Fast lookup by Customer_ID | 10,000 slots |
| Queue | Track processing order (FIFO) | Dynamic |
| Disk Buffer | Hold customer partition | 500 tuples |

| Thread | Job |
|--------|-----|
| Stream Producer | Read CSV → Stream Buffer |
| Join Consumer | Stream Buffer → Hash Table → Join → DW |

**The Result:** Each transaction is enriched with customer and product details, then loaded into the Data Warehouse for analysis!

---

*Document prepared for DS3003 & DS3004 - Data Warehousing & Business Intelligence*
