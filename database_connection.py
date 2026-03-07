from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s [%(levelname)s] %(message)s")

DB_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "dbname": os.getenv('DB_NAME')
}
if not all(DB_CONFIG.values()):
    logging.error("Database configuration is incomplete. Please check your .env file.")

class Database:
    def __init__(self, config):
        self.config = config
    def get_connection(self):
        try:
            connection = psycopg2.connect(**self.config)
            logging.info("Database connection successful")
            return connection
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            raise
    def execute_query(self, query, params:tuple = None):
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    if cursor.description:  # Check if the query returns results
                        results = cursor.fetchall()
                        logging.info(f"Query executed successfully: {query}")
                        return results
                    else:
                        logging.info(f"Query executed successfully: {query} (no results to fetch)")
                        return None
        except Exception as e:
            logging.error(f"Database query error: {e}")
            raise
    
    def truncate_table(self, table_name):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format (sql.Identifier(table_name),))
                    conn.commit()
                    logging.info(f"Table {table_name} truncated successfully.")
        except Exception as e:
            logging.error(f"Database truncate error: {e}")
            raise

    def execute_many(self, query, data):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, data)
                    conn.commit()
                    logging.info(f"Batch query executed successfully: {query}")
        except Exception as e:
            logging.error(f"Database batch query error: {e}")
            raise

if __name__ == "__main__":
    db = Database(DB_CONFIG)
    try:
        result = db.execute_query("select * from weather_table;")
        print(result)
    except Exception as e:
        logging.error(f"Error executing query: {e}")