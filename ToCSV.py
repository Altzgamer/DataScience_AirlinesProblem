import sqlite3
import csv

# Global variable N at the top
N = 260000  # You can change this value to the desired number of rows

# Connect to the SQLite database
conn = sqlite3.connect('Persons.db')
cursor = conn.cursor()

# Execute query to fetch up to N rows from the Person table
cursor.execute("SELECT * FROM Person LIMIT ?", (N,))
rows = cursor.fetchall()

# Get column headers from cursor description
headers = [desc[0] for desc in cursor.description]

# Open CSV file for writing
with open('persons.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)

    # Write headers
    writer.writerow(headers)

    # Write rows
    writer.writerows(rows)

# Close the connection
conn.close()

print(f"Up to {N} rows exported to persons.csv successfully.")