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
├── run_q1.py  - run_q20.py         # Analytical query scripts (see below)
├── q4_query.sql                    # SQL query file
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

## 📊 Analytical Queries (20 Queries)

### Basic Analytics (Q1-Q4)

| Query | File | Description |
|-------|------|-------------|
| Q1 | `run_q1.py` | Top revenue products - weekday vs weekend with monthly drill-down |
| Q2 | `run_q2.py` | Customer demographics by purchase amount with city breakdown |
| Q3 | `run_q3.py` | Additional business intelligence queries |
| Q4 | `q4_query.sql` | Direct SQL query file |

### Customer & Demographics Analytics (Q5-Q7)

| Query | File | Description |
|-------|------|-------------|
| Q5 | `run_q5.py` | **Top 5 Occupations** by product category sales |
| Q6 | `run_q6.py` | City category performance by marital status (6 months) |
| Q7 | `run_q7.py` | Average purchase by stay duration and gender |

### Revenue & City Analytics (Q8-Q10)

| Query | File | Description |
|-------|------|-------------|
| Q8 | `run_q8.py` | **Top 5 Revenue-Generating Cities** by product category |
| Q9 | `run_q9.py` | Monthly sales growth % by product category |
| Q10 | `run_q10.py` | Weekend vs weekday sales by age group |

### Product & Time Analysis (Q11-Q14)

| Query | File | Description |
|-------|------|-------------|
| Q11 | `run_q11.py` | Top revenue products weekday/weekend - monthly drill-down |
| Q12 | `run_q12.py` | Store revenue growth rate - quarterly for 2017 |
| Q13 | `run_q13.py` | Supplier sales contribution by store and product |
| Q14 | `run_q14.py` | **Seasonal analysis** (Spring/Summer/Fall/Winter) |

### Advanced Analytics (Q15-Q17)

| Query | File | Description |
|-------|------|-------------|
| Q15 | `run_q15.py` | Store-supplier monthly revenue **volatility analysis** |
| Q16 | `run_q16.py` | **Product affinity analysis** - products bought together |
| Q17 | `run_q17.py` | Yearly revenue trends with **ROLLUP** aggregation |

### Business Intelligence (Q18-Q20)

| Query | File | Description |
|-------|------|-------------|
| Q18 | `run_q18.py` | Revenue & volume analysis - **H1 vs H2** comparison |
| Q19 | `run_q19.py` | Revenue **spike detection** and outlier identification |
| Q20 | `run_q20.py` | Create **STORE_QUARTERLY_SALES** view for optimization |

### Key SQL Concepts Demonstrated

- **CTEs (Common Table Expressions)** - Q5, Q8, Q11, Q15, Q16
- **Window Functions** - ROW_NUMBER, LAG, PARTITION BY - Q9, Q12, Q15
- **ROLLUP Aggregation** - Q17
- **Conditional Aggregation** - CASE WHEN in aggregates - Q10, Q14, Q18
- **View Creation** - Q20
- **Statistical Analysis** - Volatility, outliers - Q15, Q19
- **Market Basket Analysis** - Product affinity - Q16

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
