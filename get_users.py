import psycopg2
from psycopg2 import sql

db_config = {
    'dbname': 'mockdb',
    'user': 'admin',
    'password': 'admin67',
    'host': 'localhost',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users;")

    users = cursor.fetchall()
    print("\nUsers found:")
    for user in users:
        print(user)

except Exception as e:
    print("Error connecting to the database:", e)
    import traceback
    traceback.print_exc()
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
