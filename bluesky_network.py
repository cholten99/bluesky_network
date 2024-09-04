import psycopg2
import json
import asyncio
import aiohttp
from psycopg2 import OperationalError, extras
import time  # Import time module to track execution time

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
db_config['dbname'] = 'bluesky_network'  # Ensure the database name is correct
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

# Check if tables exist and create them if not
def create_tables_if_not_exist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id SERIAL PRIMARY KEY,
                handle VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(255),
                description TEXT,
                following_count INT DEFAULT 0,  -- Number of accounts this account follows
                network_followed_count INT DEFAULT 0,  -- Number of times this account is followed by others in the network
                min_distance_from_start INT DEFAULT 0 NOT NULL  -- Minimum distance from a starting account in terms of network steps
            );
            CREATE TABLE IF NOT EXISTS connections (
                connection_id SERIAL PRIMARY KEY,
                follower_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
                following_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
                UNIQUE (follower_id, following_id)
            );
            """)
        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()

# Empty both tables
def empty_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE connections, accounts RESTART IDENTITY;")
        conn.commit()
    except Exception as e:
        print(f"Error emptying tables: {e}")
        conn.rollback()

# Fetch account information from Bluesky API using aiohttp
async def fetch_account_info(session, handle):
    url = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {bluesky_app_password}'
    }
    params = {'actor': handle}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        print(f"Error fetching account info for {handle}: {e}")
        return None

# Add account information to the database using bulk insert
def add_accounts(conn, accounts_data):
    if not accounts_data:
        print("No account data provided.")
        return
    
    # Prepare data for bulk insert
    records = [
        (
            account_data['handle'],
            account_data.get('displayName'),
            account_data.get('description'),
            account_data.get('followsCount', 0)  # Use followsCount for following_count
        )
        for account_data in accounts_data
        if account_data is not None
    ]

    # Bulk insert or update accounts
    try:
        with conn.cursor() as cursor:
            extras.execute_values(
                cursor,
                """
                INSERT INTO accounts (handle, display_name, description, following_count)
                VALUES %s
                ON CONFLICT (handle) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    following_count = EXCLUDED.following_count;
                """,
                records
            )
        conn.commit()
    except Exception as e:
        print(f"Error adding accounts to database: {e}")
        conn.rollback()

# Add connection information to the database using bulk insert
def add_connections(conn, connections_data):
    if not connections_data:
        print("No connection data provided.")
        return
    
    # Prepare data for bulk insert
    records = []
    try:
        with conn.cursor() as cursor:
            for follower, following in connections_data:
                # Fetch follower_id and following_id
                cursor.execute("SELECT account_id FROM accounts WHERE handle = %s", (follower,))
                follower_id = cursor.fetchone()
                cursor.execute("SELECT account_id FROM accounts WHERE handle = %s", (following,))
                following_id = cursor.fetchone()

                if follower_id and following_id:
                    records.append((follower_id[0], following_id[0]))

        # Bulk insert connections
        if records:
            with conn.cursor() as cursor:
                extras.execute_values(
                    cursor,
                    """
                    INSERT INTO connections (follower_id, following_id)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                    """,
                    records
                )
            conn.commit()
    except Exception as e:
        print(f"Error adding connections to database: {e}")
        conn.rollback()

# Fetch and add the info for every account followed by a specific account using asynchronous requests
async def add_followed_accounts(conn, handle):
    url = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {bluesky_app_password}'
    }
    params = {'actor': handle}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()
                following_list = await response.json()
                following_list = following_list.get('follows', [])

                # Fetch full profile info for each followed account concurrently
                tasks = [fetch_account_info(session, following['handle']) for following in following_list]
                accounts_data = await asyncio.gather(*tasks)

                # Add all fetched accounts to the database
                add_accounts(conn, accounts_data)

                # Prepare connection data for bulk insert
                connections_data = [(handle, following['handle']) for following in following_list]
                add_connections(conn, connections_data)
        except Exception as e:
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

# Calculate and print the total sum of all following_count for all accounts
def print_total_following_count_sum(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SUM(following_count) FROM accounts;")
            total_following_count = cursor.fetchone()[0]
            print(f"Total sum of all following counts for all accounts: {total_following_count}")
    except Exception as e:
        print(f"Error calculating total following count sum: {e}")

# Calculate and print the number of accounts added
def count_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM accounts;")
            total_accounts = cursor.fetchone()[0]
            print(f"Total number of accounts added: {total_accounts}")
            return total_accounts
    except Exception as e:
        print(f"Error counting accounts: {e}")
        return 0

# Main function
def main():
    # Start the timer
    start_time = time.time()
    
    # Connect to the database
    conn = connect_db()
    try:
        # Step 1: Ensure tables exist
        create_tables_if_not_exist(conn)

        # Step 2: Empty both tables
        empty_tables(conn)

        # Step 3: Add info for account "choltern.bsky.social"
        async def fetch_and_add_initial_account():
            async with aiohttp.ClientSession() as session:
                choltern_data = await fetch_account_info(session, 'choltern.bsky.social')
                add_accounts(conn, [choltern_data])

        asyncio.run(fetch_and_add_initial_account())

        # Step 4: Add info for every account "choltern.bsky.social" follows
        asyncio.run(add_followed_accounts(conn, 'choltern.bsky.social'))

        # Step 5: Write the contents of both tables to files
        write_to_file(conn)

        # Step 6: Print the total sum of all following counts for all accounts
        print_total_following_count_sum(conn)

        # Step 7: Print the total number of accounts added
        total_accounts = count_accounts(conn)

        # End the timer
        end_time = time.time()
        total_time = end_time - start_time

        # Calculate average time per account
        avg_time_per_account = total_time / total_accounts if total_accounts > 0 else 0

        # Print execution time results
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Average time per account: {avg_time_per_account:.2f} seconds")

        print("Database updated, contents written to file, and total following count sum calculated successfully.")
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()
