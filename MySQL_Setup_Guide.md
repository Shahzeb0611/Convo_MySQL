# MySQL Database Setup & Connection Guide for Cursor IDE

This guide provides step-by-step instructions for installing MySQL, connecting to your database, and using SQL within Cursor IDE.

---

## Table of Contents
1. [MySQL Installation](#1-mysql-installation)
2. [Creating Database with MySQL Workbench](#2-creating-database-with-mysql-workbench)
3. [Initial MySQL Setup (Command Line)](#3-initial-mysql-setup-command-line)
4. [Cursor IDE SQL Extension](#4-cursor-ide-sql-extension)
5. [Connecting to MySQL in Cursor](#5-connecting-to-mysql-in-cursor)
6. [Setting Up Python Virtual Environment](#6-setting-up-python-virtual-environment)
7. [Using Python with MySQL](#7-using-python-with-mysql)
8. [Running SQL Queries in Cursor](#8-running-sql-queries-in-cursor)

---

## 1. MySQL Installation

### Windows

1. **Download MySQL Installer**
   - Go to: https://dev.mysql.com/downloads/installer/
   - Download the **MySQL Installer for Windows**

2. **Run the Installer**
   - Choose **"Developer Default"** or **"Server Only"** setup type
   - Click **Next** and follow the prompts
   - **Important**: Select **MySQL Workbench** during installation (included in Developer Default)

3. **Configuration**
   - Select **"Standalone MySQL Server"**
   - Keep default port: `3306`
   - Set the **root password** (remember this!)
   - Optionally create additional user accounts

4. **Complete Installation**
   - Apply configuration
   - Finish and close installer

### macOS

```bash
# Using Homebrew
brew install mysql

# Start MySQL service
brew services start mysql

# Secure installation
mysql_secure_installation

# Install MySQL Workbench separately
brew install --cask mysqlworkbench
```

### Linux (Ubuntu/Debian)

```bash
# Update packages
sudo apt update

# Install MySQL Server
sudo apt install mysql-server

# Secure installation
sudo mysql_secure_installation

# Start MySQL service
sudo systemctl start mysql
sudo systemctl enable mysql

# Install MySQL Workbench
sudo snap install mysql-workbench-community
```

---

## 2. Creating Database with MySQL Workbench

MySQL Workbench provides a graphical interface to manage your databases without using command line.

### Opening MySQL Workbench

1. **Launch MySQL Workbench** from your applications/start menu
2. You'll see the **Home Screen** with MySQL Connections

### Connecting to MySQL Server

1. **Click on Local Instance** (or create new connection)
   - Default connection: `Local instance MySQL` on `localhost:3306`
2. **Enter your root password** when prompted
3. Click **OK** to connect

### Creating a New Database (Schema)

**Method 1: Using the Toolbar**

1. Click on the **"Create Schema"** button (cylinder icon with a + sign) in the toolbar
2. Enter your **Schema Name**: `your_database_name`
3. Leave **Default Charset** as `utf8mb4` (recommended)
4. Click **Apply**
5. Review the SQL script and click **Apply** again
6. Click **Finish**

**Method 2: Using Right-Click**

1. In the **Navigator panel** (left side), find **Schemas**
2. **Right-click** on the empty area under Schemas
3. Select **"Create Schema..."**
4. Enter the schema name and click **Apply**

**Method 3: Using SQL Query**

1. Click on the **SQL Editor** tab (or press `Ctrl+T` for new tab)
2. Type the SQL command:
   ```sql
   CREATE DATABASE your_database_name;
   ```
3. Click the **lightning bolt** icon (⚡) to execute
4. Refresh the Schemas panel to see your new database

### Creating Tables in Workbench

1. **Expand your database** in the Navigator panel
2. **Right-click on Tables** → **Create Table...**
3. Enter **Table Name**
4. Add columns:
   - Column Name
   - Data Type (INT, VARCHAR, DATE, etc.)
   - Check **PK** for Primary Key
   - Check **NN** for Not Null
   - Check **AI** for Auto Increment
5. Click **Apply** → Review SQL → **Apply** → **Finish**

### Importing SQL Files in Workbench

1. Go to **File** → **Open SQL Script** (or `Ctrl+Shift+O`)
2. Select your `.sql` file
3. Click the **lightning bolt** icon (⚡) to execute all queries
4. Or select specific queries and execute with `Ctrl+Shift+Enter`

---

## 3. Initial MySQL Setup (Command Line)

### Verify Installation

Open your terminal/command prompt and run:

```bash
mysql --version
```

### Access MySQL Command Line

```bash
mysql -u root -p
```
Enter your root password when prompted.

### Create a New Database

```sql
-- Create a new database
CREATE DATABASE your_database_name;

-- Verify it was created
SHOW DATABASES;

-- Select the database
USE your_database_name;
```

### Create a New User (Recommended)

```sql
-- Create a new user
CREATE USER 'your_username'@'localhost' IDENTIFIED BY 'your_password';

-- Grant privileges on your database
GRANT ALL PRIVILEGES ON your_database_name.* TO 'your_username'@'localhost';

-- Apply changes
FLUSH PRIVILEGES;
```

---

## 4. Cursor IDE SQL Extension

To work with SQL files and get syntax highlighting in Cursor IDE, you need to install an SQL extension.

### Required Extension: **SQLTools MySQL/MariaDB/TiDB** by mtxr

1. **Open Extensions Panel**
   - Press `Ctrl+Shift+X` (Windows/Linux) or `Cmd+Shift+X` (macOS)

2. **Search for the Extension**
   - Search: `SQLTools MySQL`
   - Find: **"SQLTools MySQL/MariaDB/TiDB"** by **mtxr**
   
3. **Install the Extension**
   - Click **Install**
   - Also install **SQLTools** (the base extension) if prompted

> 💡 **Note**: The **SQLTools MySQL/MariaDB/TiDB** extension provides excellent features including:
> - SQL syntax highlighting
> - Auto-completion and IntelliSense
> - Database explorer in sidebar
> - Query execution directly from the editor
> - Table visualization and data preview
> - Connection management

---

## 5. Connecting to MySQL in Cursor

### Using SQLTools Extension

1. **Open the SQLTools Panel**
   - Click on the **SQLTools icon** (database icon) in the sidebar
   - Or press `Ctrl+Shift+P` → Type "SQLTools: Add New Connection"

2. **Select Database Driver**
   - Choose **MySQL** from the list

3. **Configure Connection**
   Fill in the connection details:

   | Field             | Value                      |
   |-------------------|----------------------------|
   | Connection Name   | `Your Connection Name`     |
   | Server            | `localhost` or `127.0.0.1` |
   | Port              | `3306`                     |
   | Database          | `your_database_name`       |
   | Username          | `your_username`            |
   | Password          | `your_password`            |

4. **Test & Save Connection**
   - Click **"Test Connection"** to verify
   - Once successful, click **"Save Connection"**

5. **Browse Database**
   - Expand the connection in the SQLTools sidebar
   - View databases, tables, columns, and data
   - Right-click tables for options (show records, describe, etc.)

### Connection String Format

For reference, here's the typical MySQL connection format:

```
mysql://your_username:your_password@localhost:3306/your_database_name
```

---

## 6. Setting Up Python Virtual Environment

### Windows (PowerShell / Command Prompt)

```powershell
# Navigate to your project folder
cd E:\path\to\your\project

# Create virtual environment
python -m venv your_venv_name

# Activate virtual environment
your_venv_name\Scripts\activate

# You should see (your_venv_name) at the start of your prompt

# Upgrade pip (recommended)
python -m pip install --upgrade pip

# Install MySQL connector
pip install mysql-connector-python

# Create requirements.txt
pip freeze > requirements.txt

# To deactivate when done
deactivate
```

### macOS / Linux

```bash
# Navigate to your project folder
cd /path/to/your/project

# Create virtual environment
python3 -m venv your_venv_name

# Activate virtual environment
source your_venv_name/bin/activate

# You should see (your_venv_name) at the start of your prompt

# Upgrade pip (recommended)
pip install --upgrade pip

# Install MySQL connector
pip install mysql-connector-python

# Create requirements.txt
pip freeze > requirements.txt

# To deactivate when done
deactivate
```

### Using requirements.txt

Create a `requirements.txt` file:

```
mysql-connector-python>=8.0.0
```

Install from requirements:

```bash
pip install -r requirements.txt
```

### Verify Installation

```bash
# Check installed packages
pip list

# Verify mysql-connector is installed
python -c "import mysql.connector; print('MySQL Connector installed successfully!')"
```

---

## 7. Using Python with MySQL

### Python Connection Example

```python
import mysql.connector

# Database configuration - USE PLACEHOLDERS IN PRODUCTION
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database_name'
}

def connect_to_database():
    """Establish connection to MySQL database"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if connection.is_connected():
            print("✓ Successfully connected to MySQL database")
            return connection
            
    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL: {error}")
        return None

def execute_query(query):
    """Execute a SELECT query and return results"""
    connection = connect_to_database()
    
    if connection:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print column headers
        columns = [desc[0] for desc in cursor.description]
        print(columns)
        
        # Print results
        for row in results:
            print(row)
        
        cursor.close()
        connection.close()
        return results

# Example usage
if __name__ == "__main__":
    # Test connection
    conn = connect_to_database()
    
    if conn:
        cursor = conn.cursor()
        
        # Example: Get all tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print("\nTables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        
        conn.close()
```

### Using Environment Variables (Recommended for Security)

```python
import os
import mysql.connector

# Set these as environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'database': os.getenv('DB_NAME', 'your_database_name')
}
```

Set environment variables:

**Windows (PowerShell):**
```powershell
$env:DB_HOST = "localhost"
$env:DB_USER = "your_username"
$env:DB_PASSWORD = "your_password"
$env:DB_NAME = "your_database_name"
```

**macOS/Linux:**
```bash
export DB_HOST="localhost"
export DB_USER="your_username"
export DB_PASSWORD="your_password"
export DB_NAME="your_database_name"
```

---

## 8. Running SQL Queries in Cursor

### Cursor IDE Auto-Execute Feature

**Cursor IDE has the ability to automatically run and execute SQL commands** whether they are written in:
- **`.sql` files** - Direct SQL scripts
- **`.py` files** - Python files containing SQL queries

This makes it seamless to work with databases directly from your code editor.

### Running SQL from `.sql` Files

1. **Open a `.sql` file** in Cursor
2. **Connect to your database** first (via SQLTools sidebar)
3. **Execute queries**:
   - **Run entire file**: Press `Ctrl+E Ctrl+E` or right-click → "Run on Active Connection"
   - **Run selected query**: Select the query text → Press `Ctrl+E Ctrl+E`
   - **Run current statement**: Place cursor in the query → Execute

4. **View results** in the SQLTools Results panel at the bottom

### Running SQL from `.py` Files

When you have Python files with embedded SQL queries:

1. **Cursor recognizes SQL syntax** inside Python strings
2. You can **run the Python script** directly with `F5` or terminal
3. The script executes SQL through the `mysql.connector` connection
4. Results are displayed in the terminal/output panel

**Example Python file with SQL:**

```python
import mysql.connector

DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database_name'
}

# SQL query defined in Python
query = """
SELECT 
    c.Customer_ID,
    c.Name,
    SUM(t.amount) AS total_purchases
FROM customers c
JOIN transactions t ON c.Customer_ID = t.Customer_ID
GROUP BY c.Customer_ID, c.Name
ORDER BY total_purchases DESC
LIMIT 10
"""

# Execute and display
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute(query)

for row in cursor.fetchall():
    print(row)

conn.close()
```

### SQLTools Keyboard Shortcuts

| Action | Shortcut |
|--------|----------|
| Run Query | `Ctrl+E Ctrl+E` |
| Run Selected Query | Select text + `Ctrl+E Ctrl+E` |
| Format SQL | `Ctrl+E Ctrl+B` |
| Show Connection Explorer | Click SQLTools icon in sidebar |

### Example SQL File Structure

```sql
-- =====================================================
-- CREATE TABLES
-- =====================================================

-- Drop table if exists
DROP TABLE IF EXISTS your_table_name;

-- Create new table
CREATE TABLE your_table_name (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INSERT DATA
-- =====================================================

INSERT INTO your_table_name (name, email) VALUES
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com');

-- =====================================================
-- QUERY DATA
-- =====================================================

SELECT * FROM your_table_name;
```

### From Command Line

```bash
# Execute SQL file
mysql -u your_username -p your_database_name < your_script.sql

# Execute single query
mysql -u your_username -p -e "SELECT * FROM your_table" your_database_name
```

---

## Quick Reference

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | MySQL server address | `localhost` |
| `port` | MySQL server port | `3306` |
| `user` | Database username | - |
| `password` | Database password | - |
| `database` | Database name to connect | - |

### Common MySQL Commands

```sql
-- Show all databases
SHOW DATABASES;

-- Use a specific database
USE your_database_name;

-- Show all tables
SHOW TABLES;

-- Describe table structure
DESCRIBE your_table_name;

-- Show table creation script
SHOW CREATE TABLE your_table_name;
```

---

## Troubleshooting

### Common Issues

1. **"Access denied" error**
   - Verify username and password
   - Check user has proper privileges:
     ```sql
     SHOW GRANTS FOR 'your_username'@'localhost';
     ```

2. **"Can't connect to MySQL server"**
   - Ensure MySQL service is running:
     - Windows: Check Services → MySQL
     - macOS: `brew services list`
     - Linux: `sudo systemctl status mysql`

3. **"Unknown database" error**
   - Create the database first (via Workbench or command line):
     ```sql
     CREATE DATABASE your_database_name;
     ```

4. **Python module not found**
   - Ensure virtual environment is activated
   - Reinstall: `pip install mysql-connector-python`

5. **SQLTools not connecting**
   - Verify MySQL service is running
   - Check connection settings in SQLTools
   - Try "Test Connection" before saving

---

## Additional Resources

- [MySQL Official Documentation](https://dev.mysql.com/doc/)
- [MySQL Workbench Manual](https://dev.mysql.com/doc/workbench/en/)
- [MySQL Connector/Python Developer Guide](https://dev.mysql.com/doc/connector-python/en/)
- [SQLTools Extension Documentation](https://vscode-sqltools.mteixeira.dev/)
- [Video Tutorial: MySQL Setup in Cursor](https://www.youtube.com/watch?v=TfkpZzs04-I)

---

> **Security Reminder**: Never commit database credentials to version control. Use environment variables or a `.env` file (add to `.gitignore`).
