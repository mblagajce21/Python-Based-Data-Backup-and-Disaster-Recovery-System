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
    #cursor.execute("INSERT INTO users (username, email) VALUES (%s, %s);", ('newuser1', 'newuser1@example.com'))
    cursor.execute("DELETE FROM users WHERE username = %s;", ('newuser1',))  
    conn.commit()
    print("New user inserted successfully.")

except Exception as e:
    print("Error connecting to the database:", e)
    import traceback
    traceback.print_exc()
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
