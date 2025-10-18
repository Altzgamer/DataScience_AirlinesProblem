import sqlite3


def drop_person_table(db_path: str) -> None:
    """Drop the Person table from the database if it exists."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS unified_passengers")
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()
    print("table dropped successfully if it existed.")


if __name__ == "__main__":
    # Replace with your database path
    db_path = 'DataBase.db'
    drop_person_table(db_path)