import psycopg2
from psycopg2 import OperationalError

def create_db_connection(params):
    try:
        conn = psycopg2.connect(
            dbname=params['dbname'], 
            user=params['user'], 
            password=params['password'], 
            host=params['host'], 
            port=params['port']
        )
        return conn
    except OperationalError as e:
        print(f"An error occurred: {e}")
        return None
    
def close_db_connection(conn):
    if conn is not None:
        conn.close()