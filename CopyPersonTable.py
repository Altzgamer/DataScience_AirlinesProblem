import sqlite3

def copy_person_table(original_db: str, new_db: str) -> None:
    """Copy the Person table from the original database to a new database."""
    # Connect to the original database
    conn_original = sqlite3.connect(original_db)
    cursor_original = conn_original.cursor()

    cursor_original.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='Person'")
    schema = cursor_original.fetchone()
    if not schema:
        raise ValueError("Table 'Person' does not exist in the original database.")
    create_table_sql = schema[0]

    cursor_original.execute("SELECT * FROM Person")
    data = cursor_original.fetchall()

    conn_new = sqlite3.connect(new_db)
    cursor_new = conn_new.cursor()

    cursor_new.execute(create_table_sql)

    if data:
        num_columns = len(data[0])
        placeholders = ','.join('?' * num_columns)
        cursor_new.executemany(f"INSERT INTO Person VALUES ({placeholders})", data)
    conn_new.commit()
    conn_original.close()
    conn_new.close()

if __name__ == "__main__":
    original_db = 'DataBase.db'
    new_db = 'Persons.db'
    copy_person_table(original_db, new_db)
    print(f"Copied 'Person' table from '{original_db}' to '{new_db}'.")