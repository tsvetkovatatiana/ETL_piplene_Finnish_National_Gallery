import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
            host=os.getenv("HOST"),
            dbname=os.getenv("DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("PASSWORD"),
            port=os.getenv("PORT"),
            sslmode="require"
    )