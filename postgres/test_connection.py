import os
import psycopg2

try:
    connection = psycopg2.connect(
        host="localhost",
        port=5432,
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    cursor = connection.cursor()
    # Execute a test query
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS sample_table (name VARCHAR(5) NOT NULL, age INTEGER NOT NULL);"
    )
    connection.commit()
    # db_version = cursor.fetchone()
    # print(f"Connected to database. PostgreSQL version: {db_version}")

except Exception as error:
    print(f"Error connecting to database: {error}")

finally:
    if connection:
        cursor.close()
        connection.close()
