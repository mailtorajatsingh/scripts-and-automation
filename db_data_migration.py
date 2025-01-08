import pymysql
import pyodbc
import platform
import socket
from datetime import datetime

# MySQL Connection Function
def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="Password",
            database="DabaseName"
        )
        print("Successfully connected to MySQL database!")
        return connection
    except pymysql.MySQLError as err:
        print(f"Error: {err}")
        return None

# SQL Server Connection Function
def connect_to_sqlserver():
    try:
        server = '127.0.0.1'
        database = 'DabaseName'
        username = 'SA'
        password = 'Password'
        driver = 'ODBC Driver 18 for SQL Server'

        conn_str = (
            f'DRIVER={{{driver}}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'TrustServerCertificate=yes;'
        )

        print("System Diagnostics:")
        print(f"Operating System: {platform.platform()}")
        print(f"Python Version: {platform.python_version()}")
        print(f"Hostname: {socket.gethostname()}")
        print(f"Using ODBC Driver: {driver}")

        connection = pyodbc.connect(conn_str)
        print("Successfully connected to SQL Server!")
        return connection
    except pyodbc.Error as e:
        print(f"SQL Server Connection Error: {e}")
        return None

def create_mysql_table_if_not_exists(mysql_conn, mysql_cursor, sql_cursor, table_name):
    print(f"Checking if table '{table_name}' exists in MySQL...")

    # Check if table exists in MySQL
    mysql_cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if mysql_cursor.fetchone():
        print(f"Table '{table_name}' already exists in MySQL.")
        return

    print(f"Table '{table_name}' does not exist. Creating schema...")

    # # Fetch table schema from SQL Server OLD
    # sql_cursor.execute(f"SELECT TOP 1 * FROM {table_name}")

    # Fetch table schema from SQL Server 
    sql_cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    """)
    # columns_info = sql_cursor.description
    columns_info = sql_cursor.fetchall()

    # Fetch primary key columns for the table
    primary_keys_query = f"""
        SELECT KU.COLUMN_NAME 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU 
            ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
            AND TC.TABLE_NAME = KU.TABLE_NAME
        WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND TC.TABLE_NAME = '{table_name}'
    """
    sql_cursor.execute(primary_keys_query)
    primary_keys = [row[0] for row in sql_cursor.fetchall()]

    # Fetch unique key columns for the table
    unique_keys_query = f"""
        SELECT KU.COLUMN_NAME 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU 
            ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
            AND TC.TABLE_NAME = KU.TABLE_NAME
        WHERE TC.CONSTRAINT_TYPE = 'UNIQUE'
          AND TC.TABLE_NAME = '{table_name}'
    """
    sql_cursor.execute(unique_keys_query)
    unique_keys = [row[0] for row in sql_cursor.fetchall()]

    # Data type mapping: SQL Server -> MySQL
    # type_mapping = {
    #     'int': 'INT',
    #     'smallint': 'SMALLINT',
    #     'bigint': 'BIGINT',
    #     'varchar': 'VARCHAR(512)',
    #     'nvarchar': 'VARCHAR(512)',
    #     'char': 'CHAR(1)',
    #     'float': 'FLOAT',
    #     'datetime': 'DATETIME',
    #     'text': 'TEXT',
    #     'bit': 'BOOLEAN',
    #     'tinyint': 'TINYINT'
    # }

    type_mapping = {
    'int': 'INT',
    'smallint': 'SMALLINT',
    'bigint': 'BIGINT',
    'varchar': 'VARCHAR',
    'nvarchar': 'VARCHAR',
    'char': 'CHAR(1)',
    'float': 'FLOAT',
    'datetime': 'DATETIME',
    'datetime2': 'DATETIME(6)',  # Mapping datetime2 to DATETIME with precision
    'text': 'TEXT',
    'bit': 'BOOLEAN',
    'tinyint': 'TINYINT',
    'image': 'LONGBLOB'  # Map 'image' to a valid MySQL type
  }


    # Generate column definitions
    # Generate column definitions
    column_defs = []
    for col in columns_info:
        col_name = col[0]
        col_type = col[1].lower()
        max_length = col[2]

        if col_type in ['varchar', 'nvarchar']:
            # Had to do this conversion as error occurs because the total row size, including all VARCHAR columns' storage, exceeds MySQL's limit of 65,535 bytes for a single row.
            if max_length and max_length > 0:
              if max_length > 21845:
                  mysql_type = "TEXT"
              else:
                  mysql_type = f"{type_mapping[col_type]}({max_length})"
            else:
                mysql_type = f"{type_mapping[col_type]}(512)"  # Default length if not specified
        else:
            mysql_type = type_mapping.get(col_type, 'VARCHAR(512)')  # Default type if unknown
        column_defs.append(f"`{col_name}` {mysql_type}")

    # Add PRIMARY KEY definition if primary keys exist
    if primary_keys:
        column_defs.append(f"PRIMARY KEY ({', '.join([f'`{pk}`' for pk in primary_keys])})")

    # Add UNIQUE KEY definitions if unique keys exist
    if unique_keys:
        for unique_key in unique_keys:
            column_defs.append(f"UNIQUE KEY `unique_{unique_key}` (`{unique_key}`)")

    # Combine column definitions and constraints
    create_table_query = f"""
    CREATE TABLE `{table_name}` (
        {', '.join(column_defs)}
    )
    """

    print(f"Creating MySQL table with query:\n{create_table_query}")
    mysql_cursor.execute(create_table_query)
    print(f"Table '{table_name}' created successfully in MySQL.")


# Convert datetime to string for MySQL compatibility
def convert_to_mysql_datetime(dt):
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt

# Debugging Function for Insertion
def debug_insert(mysql_cursor, insert_query, row):
    try:
        ###print(f"Attempting to insert: {row}")
        mysql_cursor.execute(insert_query, list(row))
        ###print("Row inserted successfully.")
    except Exception as e:
        print(f"Error during insertion: {e}")
        print(f"Failed to insert row: {row}")
        raise  # Reraise the exception to handle it at a higher level

# Main Full Data Load Function
def full_data_load():
    table_name = "products"

    # Connect to SQL Server
    sql_conn = connect_to_sqlserver()
    if not sql_conn:
        print("Failed to connect to SQL Server. Exiting...")
        return
    sql_cursor = sql_conn.cursor()

    # Connect to MySQL
    mysql_conn = connect_to_mysql()
    if not mysql_conn:
        print("Failed to connect to MySQL. Exiting...")
        sql_cursor.close()
        sql_conn.close()
        return
    mysql_cursor = mysql_conn.cursor()

    try:
        # Step 1: Check and create table schema in MySQL
        create_mysql_table_if_not_exists(mysql_conn, mysql_cursor, sql_cursor, table_name)

        # Step 2: Fetch data from SQL Server
        sql_cursor.execute(f"SELECT * FROM {table_name}")
        rows = sql_cursor.fetchall()  

        # Step 3: Insert data into MySQL
        for row in rows:
            # Convert datetime fields to MySQL-compatible format
            row = [convert_to_mysql_datetime(val) if isinstance(val, datetime) else val for val in row]

            placeholders = ", ".join(["%s"] * len(row))  # Generates %s placeholders
            columns = ", ".join([f"`{desc[0]}`" for desc in sql_cursor.description])  # Safe column names
            insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

            # Debug insertion and try until success
            retry_attempts = 3
            for attempt in range(retry_attempts):
                try:
                    ###print(f"Inserting row: {row}")  # Print the row being inserted for debugging
                    debug_insert(mysql_cursor, insert_query, row)
                    mysql_conn.commit()  # Commit after each row is inserted
                    break  # If successful, break the retry loop
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed. Retrying...")
                    if attempt == retry_attempts - 1:
                        print("Max retry attempts reached. Skipping this row.")

        # Commit to MySQL after all rows are inserted
        print(f"Committing all changes to MySQL.")
        mysql_conn.commit()
        print("Full data load completed successfully!")

    except Exception as e:
        print(f"Error during data load: {e}")

    finally:
        # Close connections
        if sql_cursor:
            sql_cursor.close()
        if sql_conn:
            sql_conn.close()
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()
        print("Database connections closed.")

# Run the Full Data Load
if __name__ == "__main__":
    full_data_load()
