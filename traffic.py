import mysql.connector

# Establish connection to the MySQL database
connection = mysql.connector.connect(
    host='your_host',  # e.g., 'localhost' or '127.0.0.1'
    user='your_username',  # e.g., 'root'
    password='your_password',  # Your MySQL password
    database='your_database'  # The name of your database
)

# Create a cursor object
cursor = connection.cursor()

# Define traffic query
query = """
WITH traffic_cte AS (
    SELECT client, protocol,
           (COALESCE(SUM(traffic_in), 0) + COALESCE(SUM(traffic_out), 0)) AS total_traffic
    FROM traffic
    GROUP BY client, protocol
)
SELECT client,
       GROUP_CONCAT(protocol ORDER BY total_traffic DESC) AS protocol
FROM traffic_cte
GROUP BY client
ORDER BY client ASC;
"""

# Execute the query
cursor.execute(query)

# Fetch all the results
results = cursor.fetchall()

# Print the header
print(f"{'Client':<20} | {'Protocol'}")
print("-" * 40)

# Print the results
for row in results:
    print(f"{row[0]:<20} | {row[1]}")

# Close the cursor and connection
cursor.close()
connection.close()
