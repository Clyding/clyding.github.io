from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor

def mongodb_connection(db_name, collection_name):
    client = MongoClient('mongodb://admin:admin@mongodb:27017/')

    db = client[db_name]
    collection = db[collection_name]

    return db, collection

def postgre_connection():
    conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",  
    port="5432"      
)

    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    return conn, cur