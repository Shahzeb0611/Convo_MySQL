"""
Script to load CSV data into MySQL temp tables
Run this AFTER running the CREATE TABLE statements from database.sql
"""

import csv
import mysql.connector
from mysql.connector import Error

# =====================================================
# DATABASE CONNECTION SETTINGS - UPDATE THESE
# =====================================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'project_test'
}

# File paths
DATA_FOLDER = r'E:\CONVO\Misc\Curso_MySQL\29-1-26\data'


def load_customer_data(cursor, connection):
    """Load customer data into temp_customer"""
    print("Loading temp_customer data...")
    
    filepath = f"{DATA_FOLDER}\\customer_master_data.csv"
    
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        
        insert_query = """
            INSERT INTO temp_customer 
            (row_index, customer_id, gender, age, occupation, city_category, stay_years, marital_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch = []
        count = 0
        for row in reader:
            batch.append(tuple(row))
            count += 1
            
            if len(batch) >= 1000:
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []
                print(f"  Inserted {count} rows...")
        
        if batch:
            cursor.executemany(insert_query, batch)
            connection.commit()
    
    print(f"✓ temp_customer: {count} rows inserted")


def load_product_data(cursor, connection):
    """Load product data into temp_master"""
    print("Loading temp_master data...")
    
    filepath = f"{DATA_FOLDER}\\product_master_data.csv"
    
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        
        insert_query = """
            INSERT INTO temp_master 
            (row_index, product_id, product_category, price, store_id, supplier_id, store_name, supplier_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch = []
        count = 0
        for row in reader:
            batch.append(tuple(row))
            count += 1
            
            if len(batch) >= 1000:
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []
                print(f"  Inserted {count} rows...")
        
        if batch:
            cursor.executemany(insert_query, batch)
            connection.commit()
    
    print(f"✓ temp_master: {count} rows inserted")


def load_transactional_data(cursor, connection):
    """Load transactional data into temp_transactions"""
    print("Loading temp_transactions... (this may take a while)")
    
    filepath = f"{DATA_FOLDER}\\transactional_data.csv"
    
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        
        insert_query = """
            INSERT INTO temp_transactions 
            (row_index, order_id, customer_id, product_id, quantity, full_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        batch = []
        count = 0
        for row in reader:
            batch.append(tuple(row))
            count += 1
            
            if len(batch) >= 5000:
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []
                if count % 50000 == 0:
                    print(f"  Inserted {count} rows...")
        
        if batch:
            cursor.executemany(insert_query, batch)
            connection.commit()
    
    print(f"✓ temp_transactions: {count} rows inserted")


def main():
    connection = None
    try:
        print("Connecting to MySQL database...")
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if connection.is_connected():
            cursor = connection.cursor()
            print("✓ Connected successfully!\n")
            
            # Load data into temp tables
            load_customer_data(cursor, connection)
            load_product_data(cursor, connection)
            load_transactional_data(cursor, connection)
            
            print("\n" + "="*50)
            print("All temp tables loaded successfully!")
            print("="*50)
            
            # Verify counts
            cursor.execute("SELECT COUNT(*) FROM temp_customer")
            print(f"temp_customer: {cursor.fetchone()[0]} rows")
            
            cursor.execute("SELECT COUNT(*) FROM temp_master")
            print(f"temp_master: {cursor.fetchone()[0]} rows")
            
            cursor.execute("SELECT COUNT(*) FROM temp_transactions")
            print(f"temp_transactions: {cursor.fetchone()[0]} rows")
            
            print("\n" + "="*50)
            print("Now run the INSERT statements in database.sql")
            print("to populate dimension and fact tables.")
            print("="*50)
            
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
