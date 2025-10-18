import sqlite3


def create_truncated_copy(original_db_path, new_db_path, x):
    """
    Создаёт копию базы данных, оставляя в каждой таблице только первые X записей.

    :param original_db_path: Путь к исходной БД (например, 'your_database.db')
    :param new_db_path: Путь к новой БД (например, 'truncated_copy.db')
    :param x: Количество записей для сохранения в каждой таблице
    """

    conn_original = sqlite3.connect(original_db_path)
    cursor_original = conn_original.cursor()

    conn_new = sqlite3.connect(new_db_path)
    cursor_new = conn_new.cursor()

    cursor_original.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor_original.fetchall() if row[0] != 'sqlite_sequence']  # Исключаем системные

    for table in tables:
        cursor_original.execute(f"SELECT sql FROM sqlite_master WHERE name='{table}';")
        create_sql = cursor_original.fetchone()[0]

        cursor_new.execute(create_sql)

        cursor_original.execute(f"SELECT * FROM {table} LIMIT {x};")
        rows = cursor_original.fetchall()

        if rows:
            # Получение количества колонок
            num_columns = len(rows[0])
            placeholders = ','.join(['?'] * num_columns)
            cursor_new.executemany(f"INSERT INTO {table} VALUES ({placeholders});", rows)

    conn_new.commit()

    conn_original.close()
    conn_new.close()

    print(f"Копия БД '{new_db_path}' создана с первыми {x} записями в каждой таблице.")

create_truncated_copy('DataBase.db', 'ShortDataBase.db', 15000)  # Оставит первые 10 записей