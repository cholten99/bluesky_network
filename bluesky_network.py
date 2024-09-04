import psycopg2
import json
import requests
from psycopg2 import sql, OperationalError

# Load configuration from config.json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("Error: config.json file not found.")
    exit(1)
except json.JSONDecodeError:
    print("Error: Failed to decode JSON from config.json.")
    exit(1)

db_config = config['db_config']
bluesky_app_password = config['bluesky_config']['app_password']

# Connect to the PostgreSQL database
def connect_db():
    try:
        return psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

# Empty both tables
def empty_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE connections, accounts RESTART IDENTITY;")
        conn.commit()
    except Exception as e:
        print(f"Error emptying tables: {e}")
        conn.rollback()

# Fetch account information from Bluesky API
def fetch_account_info(handle):
    try:
        response = requests.get(
            f'https://bsky.social/api/v1/accounts/{handle}',
            headers={'Authorization': f'Bearer {bluesky_app_password}'}
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching account info for {handle}: {e}")
        return None

# Add account information to the database
def add_account(conn, account_data):
    if not account_data:
        print("No account data provided.")
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO accounts (handle, display_name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (handle) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description;
                """,
                (account_data['handle'], account_data.get('display_name'), account_data.get('description'))
            )
        conn.commit()
    except Exception as e:
        print(f"Error adding account {account_data['handle']} to database: {e}")
        conn.rollback()

# Add connection information to the database
def add_connection(conn, follower_handle, following_handle):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO connections (follower_id, following_id)
                SELECT f.account_id, fo.account_id
                FROM accounts f, accounts fo
                WHERE f.handle = %s AND fo.handle = %s
                ON CONFLICT DO NOTHING;
                """,
                (follower_handle, following_handle)
            )
        conn.commit()
    except Exception as e:
        print(f"Error adding connection from {follower_handle} to {following_handle}: {e}")
        conn.rollback()

# Fetch and add the info for every account followed by a specific account
def add_followed_accounts(conn, handle):
    try:
        response = requests.get(
            f'https://bsky.social/api/v1/accounts/{handle}/following',
            headers={'Authorization': f'Bearer {bluesky_app_password}'}
        )
        response.raise_for_status()
        following_list = response.json()
        
        for following in following_list:
            add_account(conn, following)  # Add each followed account
            add_connection(conn, handle, following['handle'])  # Update connections table
    except requests.RequestException as e:
        print(f"Error fetching followed accounts for {handle}: {e}")

# Write the contents of both tables to a file
def write_to_file(conn):
    try:
        with conn.cursor() as cursor:
            # Write accounts table
            cursor.execute("SELECT * FROM accounts;")
            accounts = cursor.fetchall()
            with open('accounts_data.txt', 'w') as f:
                for row in accounts:
                    f.write(f"{row}\n")

            # Write connections table
            cursor.execute("SELECT * FROM connections;")
            connections = cursor.fetchall()
            with open('connections_data.txt', 'w') as f:
                for row in connections:
                    f.write(f"{row}\n")
    except Exception as e:
        print(f"Error writing to file: {e}")

# Main function
def main():
    # Connect to the database
    conn = connect_db()
    try:
        # Step 1: Empty both tables
        empty_tables(conn)

        # Step 2: Add info for account "choltern.bsky.social"
        choltern_data = fetch_account_info('choltern.bsky.social')
        add_account(conn, choltern_data)

        # Step 3: Add info for every account "choltern.bsky.social" follows
        add_followed_accounts(conn, 'choltern.bsky.social')

        # Step 4: Write the contents of both tables to files
        write_to_file(conn)

        print("Database updated and contents written to file successfully.")
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()
