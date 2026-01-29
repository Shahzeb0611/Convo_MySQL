# MySQL Course Project - Retail Analytics Database

A comprehensive MySQL database project for learning database design, data loading, and analytical queries using retail/e-commerce data.

---

## 📋 Project Overview

This project demonstrates:
- **Database Design**: Relational schema with proper foreign key constraints
- **Data Loading**: Python scripts to load CSV data into MySQL
- **Analytical Queries**: Complex SQL queries with CTEs, window functions, and aggregations

---

## 🗂️ Database Schema

The database consists of three main tables:

### 1. `customer_master`
| Column | Type | Description |
|--------|------|-------------|
| Customer_ID | INT (PK) | Unique customer identifier |
| Gender | CHAR(1) | M/F |
| Age | VARCHAR(10) | Age group |
| Occupation | INT | Occupation code |
| City_Category | CHAR(1) | City category (A/B/C) |
| Stay_In_Current_City_Years | VARCHAR(5) | Years in current city |
| Marital_Status | TINYINT | 0 or 1 |

### 2. `product_master`
| Column | Type | Description |
|--------|------|-------------|
| Product_ID | VARCHAR(20) (PK) | Unique product identifier |
| Product_Category | VARCHAR(50) | Product category |
| price | DECIMAL(10,2) | Product price |
| storeID | INT | Store identifier |
| supplierID | INT | Supplier identifier |
| storeName | VARCHAR(100) | Store name |
| supplierName | VARCHAR(100) | Supplier name |

### 3. `transactional_data`
| Column | Type | Description |
|--------|------|-------------|
| orderID | INT (PK) | Unique order identifier |
| Customer_ID | INT (FK) | Reference to customer |
| Product_ID | VARCHAR(20) (FK) | Reference to product |
| quantity | INT | Quantity ordered |
| order_date | DATE | Date of order |

---

## 📁 Project Structure

```
├── data/
│   ├── customer_master_data.csv    # Customer data (~5,800 records)
│   ├── product_master_data.csv     # Product data (~3,600 records)
│   └── transactional_data.csv      # Transaction data (~550,000 records)
├── create_tables.sql               # DDL for creating tables
├── database.sql                    # Full database schema
├── insert_data.sql                 # Sample insert statements
├── load_data.py                    # Python script to load CSV data
├── run_q1.py                       # Query: Top revenue products (weekday/weekend)
├── run_q2.py                       # Query: Customer demographics analysis
├── run_q3.py                       # Query: Additional analytics
├── q4_query.sql                    # SQL query file
├── run_q5.py                       # Query: Top occupations by product category
├── run_q6.py                       # Query: City category by marital status
├── run_q7.py                       # Query: Purchase by stay duration & gender
├── requirements.txt                # Python dependencies
└── MySQL_Setup_Guide.md            # Detailed setup instructions
```

---

## 🚀 Getting Started

### Prerequisites
- MySQL Server 8.0+
- Python 3.8+
- MySQL Workbench (optional, for GUI)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Shahzeb0611/Convo_MySQL.git
   cd Convo_MySQL
   ```

2. **Create a Python virtual environment**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # source venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create the database in MySQL**
   ```sql
   CREATE DATABASE project_test;
   USE project_test;
   ```

5. **Run the table creation script**
   - Execute `create_tables.sql` in MySQL Workbench or command line

6. **Load the data**
   - Update `DB_CONFIG` in `load_data.py` with your MySQL credentials
   - Run: `python load_data.py`

---

## 📊 Analytical Queries

### Q1: Top Revenue-Generating Products (`run_q1.py`)
Analyzes top products by revenue, comparing weekday vs weekend performance with monthly drill-down.

### Q2: Customer Demographics (`run_q2.py`)
Analyzes customer demographics by purchase amount with city category breakdown. Includes summaries by gender, age group, and city category.

### Q3: Additional Analytics (`run_q3.py`)
Contains additional business intelligence queries.

### Q4: SQL Query (`q4_query.sql`)
Direct SQL query file for execution in MySQL Workbench or command line.

### Q5: Top Occupations by Product Category Sales (`run_q5.py`)
Highlights the **top 5 occupations** driving sales within each product category. Features:
- Ranked occupations per category by total sales
- Overall top 10 occupations summary
- Occupation diversity analysis (categories purchased)

### Q6: City Category Performance by Marital Status (`run_q6.py`)
Assesses purchase amounts by **city category and marital status** over the past 6 months. Features:
- Monthly breakdown by city and marital status
- Summary totals with averages per transaction and per customer
- Cross-tab analysis with percentage breakdown

### Q7: Average Purchase Amount by Stay Duration and Gender (`run_q7.py`)
Calculates the **average purchase amount** based on years stayed in the city and gender. Features:
- Detailed breakdown by stay duration and gender
- Summary by stay duration (all genders)
- Summary by gender (all durations)
- Cross-tab with spending comparison insights

---

## ⚙️ Configuration

Update the database connection settings in the Python files:

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',
    'database': 'project_test'
}
```

---

## 📚 Documentation

For detailed setup instructions including:
- MySQL installation (Windows/Mac/Linux)
- MySQL Workbench usage
- Cursor IDE SQL extension setup
- Python virtual environment configuration

See: [MySQL_Setup_Guide.md](MySQL_Setup_Guide.md)

---

## 🛠️ Technologies Used

- **Database**: MySQL 8.0
- **Language**: Python 3.x, SQL
- **Libraries**: mysql-connector-python
- **Tools**: MySQL Workbench, Cursor IDE

---

## 📄 License

This project is for educational purposes as part of a MySQL course.

---

## 👤 Author

**Shahzeb** - [GitHub](https://github.com/Shahzeb0611)
