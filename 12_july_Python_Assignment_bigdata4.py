#!/usr/bin/env python
# coding: utf-8

# 1.  Write a Python program that uses the HiveQL language to create a table named "Employees" with columns for "id," "name," and "salary."
# 2.  Create a Python program that retrieves records from a Hive table named "Customers" where the age is greater than 30.
# 3.  Write a Python script that sorts records in descending order based on the "timestamp" column in a Hive table named "Logs."
# 4.  Write a Python program that connects to a Hive server using PyHive library and retrieves all records from a table named "Products".
# 5.  Write a Python script that calculates the average salary of employees from a Hive table named "Employees".
# 6.  Implement a Python program that uses Hive partitioning to create a partitioned table named "Sales_Data" based on the "year" and "month" columns.
# 7.  Develop a Python script that adds a new column named "email" of type string to an existing Hive table named "Employees."
# 8.  Create a Python program that performs an inner join between two Hive tables, "Orders" and "Customers," based on a common column.
# 9.  Implement a Python program that uses the Hive SerDe library to process JSON data stored in a Hive table named "User_Activity_Logs."
# 
# 
# 
# 

# In[3]:


pip install pyhive


# In[2]:


pip install thrift


# In[2]:


pip install sasl


# In[3]:


#1ANS
from pyhive import hive

def create_employees_table():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to create the Employees table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS Employees (
            id INT,
            name STRING,
            salary FLOAT
        )
    """

    # Execute the query to create the Employees table
    cursor.execute(create_table_query)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Create the Employees table
create_employees_table()


# In[4]:


#2ANS
from pyhive import hive

def retrieve_customers_with_age_greater_than_30():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to retrieve customers with age greater than 30
    retrieve_query = """
        SELECT *
        FROM Customers
        WHERE age > 30
    """

    # Execute the query to retrieve the customers
    cursor.execute(retrieve_query)

    # Fetch all the records
    results = cursor.fetchall()

    # Print the records
    for row in results:
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Retrieve customers with age greater than 30
retrieve_customers_with_age_greater_than_30()


# In[5]:


#3ANS
from pyhive import hive

def sort_logs_by_timestamp_desc():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to sort logs by timestamp in descending order
    sort_query = """
        SELECT *
        FROM Logs
        ORDER BY timestamp DESC
    """

    # Execute the query to sort the logs
    cursor.execute(sort_query)

    # Fetch all the sorted records
    results = cursor.fetchall()

    # Print the sorted records
    for row in results:
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Sort logs by timestamp in descending order
sort_logs_by_timestamp_desc()


# In[6]:


#4ANS
from pyhive import hive

def retrieve_products():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to retrieve all records from the "Products" table
    retrieve_query = """
        SELECT *
        FROM Products
    """

    # Execute the query to retrieve the records
    cursor.execute(retrieve_query)

    # Fetch all the records
    results = cursor.fetchall()

    # Print the records
    for row in results:
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Retrieve all records from the "Products" table
retrieve_products()


# In[7]:


#5ANS
from pyhive import hive

def calculate_average_salary():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to calculate the average salary
    avg_salary_query = """
        SELECT AVG(salary) AS average_salary
        FROM Employees
    """

    # Execute the query to calculate the average salary
    cursor.execute(avg_salary_query)

    # Fetch the average salary
    result = cursor.fetchone()

    # Extract the average salary value
    average_salary = result[0]

    # Print the average salary
    print(f"Average Salary: {average_salary}")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Calculate the average salary of employees
calculate_average_salary()


# In[8]:


#6ANS
from pyhive import hive

def create_partitioned_table():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to create the partitioned table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS Sales_Data (
            -- Columns here
        )
        PARTITIONED BY (year INT, month INT)
    """

    # Execute the query to create the partitioned table
    cursor.execute(create_table_query)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Create the partitioned table
create_partitioned_table()


# In[9]:


#7ANS
from pyhive import hive

def add_column_to_table():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to add a new column to the "Employees" table
    add_column_query = """
        ALTER TABLE Employees
        ADD COLUMNS (email STRING)
    """

    # Execute the query to add the new column
    cursor.execute(add_column_query)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Add the new column to the table
add_column_to_table()


# In[10]:


#8ANS
from pyhive import hive

def perform_inner_join():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Define the HiveQL query to perform the inner join
    inner_join_query = """
        SELECT *
        FROM Orders
        INNER JOIN Customers
        ON Orders.customer_id = Customers.customer_id
    """

    # Execute the query to perform the inner join
    cursor.execute(inner_join_query)

    # Fetch all the joined records
    results = cursor.fetchall()

    # Print the joined records
    for row in results:
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Perform the inner join between "Orders" and "Customers" tables
perform_inner_join()


# In[11]:


#9ANS
from pyhive import hive

def process_json_data():
    # Connect to Hive
    conn = hive.Connection(host='localhost', port=10000, username='your_username')

    # Create a cursor
    cursor = conn.cursor()

    # Enable Hive SerDe library for JSON data
    enable_json_serde_query = """
        ADD JAR /path/to/json-serde.jar
    """

    # Execute the query to enable the Hive SerDe library for JSON data
    cursor.execute(enable_json_serde_query)

    # Define the HiveQL query to process JSON data
    process_json_query = """
        SELECT *
        FROM User_Activity_Logs
    """

    # Execute the query to process JSON data
    cursor.execute(process_json_query)

    # Fetch all the processed records
    results = cursor.fetchall()

    # Print the processed records
    for row in results:
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Process JSON data stored in the "User_Activity_Logs" table
process_json_data()


# In[ ]:




