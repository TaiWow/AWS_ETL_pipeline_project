import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
host_name = os.getenv("POSTGRES_HOST", "localhost")
user_name = os.getenv("POSTGRES_USER", "yourusername")
user_password = os.getenv("POSTGRES_PASSWORD", "yourpassword")
database_name = os.getenv("POSTGRES_DB", "nubi_project_db")
port_number = int(os.getenv("POSTGRES_PORT", 5432))


def print_separator():
    print("------------")

def setup_db_connection():
    try:
        connection = psycopg2.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            dbname=database_name,
            port=port_number
        )
        print("Database connection successful.")
        print_separator()
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print_separator()
        return None

if __name__ == '__main__':
    connection = setup_db_connection()