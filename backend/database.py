import psycopg2
from psycopg2.extras import RealDictCursor

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bic_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres123"  # replace with your PostgreSQL password

def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor
    )
    return conn
