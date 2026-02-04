import time
import requests
from tqdm import tqdm
import pandas as pd
import duckdb

def create_user_info_table(conn, github_token):

    # Get GitHub API token
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable not set")

    # Get developers to process from canonical_developers that are not yet in user_info
    existing_count = conn.execute("SELECT count(*) FROM user_info").fetchone()[0]
    print(f"Found {existing_count} existing developers in database")

    df_to_process = conn.execute("""
        SELECT id, primary_github_user_id
        FROM canonical_developers
        WHERE id NOT IN (SELECT canonical_developer_id FROM user_info)
    """).df()
    print(f"Will process {len(df_to_process)} new developers")

    if len(df_to_process) == 0:
        print("No new developers to process!")
    else:
        # Process in batches of 100
        batch_size = 100
        total_batches = (len(df_to_process) + batch_size - 1) // batch_size
        
        # Track rate limit state
        rate_limit_remaining = 5000
        last_reset_time = 0
        min_rate_limit_buffer = 100
        
        with tqdm(total=len(df_to_process), desc="Processing developers", unit="dev") as pbar:
            for i in range(0, len(df_to_process), batch_size):
                batch_df = df_to_process.iloc[i:i+batch_size]
                
                # Extract primary_github_user_ids for this batch (filter out NaN values)
                node_ids = batch_df['primary_github_user_id'].dropna().tolist()
                
                # Create mapping from primary_github_user_id to canonical_developer_id for this batch
                id_mapping = dict(zip(batch_df['primary_github_user_id'], batch_df['id']))
                
                # Skip if no valid node_ids in this batch
                if not node_ids:
                    # Still need to insert rows without primary_github_user_id if they exist
                    for idx, row in batch_df.iterrows():
                        if pd.isna(row['primary_github_user_id']):
                            conn.execute("""
                                INSERT INTO user_info 
                                (canonical_developer_id, login, name, company, location, url, email, primary_github_user_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (canonical_developer_id) DO NOTHING
                            """, [
                                row['id'],
                                None, None, None, None, None, None, None
                            ])
                    pbar.update(len(batch_df))
                    continue
                
                # Check if we need to wait due to rate limits
                current_time = int(time.time())
                if rate_limit_remaining < min_rate_limit_buffer:
                    if last_reset_time > current_time:
                        wait_time = last_reset_time - current_time + 5
                        print(f"\nRate limit low ({rate_limit_remaining} remaining). Waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        wait_time = 60
                        print(f"\nRate limit low ({rate_limit_remaining} remaining). Waiting {wait_time}s...")
                        time.sleep(wait_time)
                
                try:
                    # Fetch user info from GitHub API
                    users_batch, rate_limit_info = get_github_users_by_node_ids_query(
                        node_ids, 
                        github_token
                    )
                    
                    # Update rate limit tracking
                    rate_limit_remaining = rate_limit_info.get('remaining', 0)
                    last_reset_time = rate_limit_info.get('reset_time', 0)
                    
                    # Create a dict mapping primary_github_user_id to user data for quick lookup
                    users_by_primary_id = {}
                    for user in users_batch:
                        if user and user.get('primary_github_user_id'):
                            users_by_primary_id[user['primary_github_user_id']] = user
                    
                    # Insert each row from the batch into the database
                    for idx, row in batch_df.iterrows():
                        canonical_id = row['id']
                        primary_id = row['primary_github_user_id']
                        
                        # Get user info if available
                        if pd.notna(primary_id) and primary_id in users_by_primary_id:
                            user = users_by_primary_id[primary_id]
                            conn.execute("""
                                INSERT INTO user_info 
                                (canonical_developer_id, login, name, company, location, url, email, primary_github_user_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (canonical_developer_id) DO NOTHING
                            """, [
                                canonical_id,
                                user.get('login'),
                                user.get('name'),
                                user.get('company'),
                                user.get('location'),
                                user.get('url'),
                                user.get('email'),
                                primary_id
                            ])
                        else:
                            # Insert row with canonical_id even if no GitHub user data
                            conn.execute("""
                                INSERT INTO user_info 
                                (canonical_developer_id, login, name, company, location, url, email, primary_github_user_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (canonical_developer_id) DO NOTHING
                            """, [
                                canonical_id,
                                None, None, None, None, None, None,
                                primary_id if pd.notna(primary_id) else None
                            ])
                    
                    # Adaptive throttling based on rate limit
                    if rate_limit_remaining < min_rate_limit_buffer * 2:
                        sleep_time = 1.0
                    elif rate_limit_remaining < min_rate_limit_buffer * 5:
                        sleep_time = 0.5
                    else:
                        sleep_time = 0.1
                    
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    print(f"\nError processing batch {i//batch_size + 1}: {e}")
                    import traceback
                    traceback.print_exc()
                    # Wait a bit before retrying
                    time.sleep(5)
                
                # Update progress bar
                pbar.update(len(batch_df))
                
                # Log rate limit status periodically
                if (i // batch_size) % 100 == 0 and i > 0:
                    pbar.set_postfix({
                        'rate_limit': rate_limit_remaining,
                        'batch': f"{i//batch_size + 1}/{total_batches}"
                    })
        
        print(f"\nCompleted processing {len(df_to_process)} developers")


def get_github_users_by_node_ids_query(node_ids, api_token, max_retries=5):
    """
    Fetches a list of GitHub users based on their GraphQL Node IDs.
    Includes the original node_id in each result (even if None).
    Implements retry logic with exponential backoff for rate limit errors.
    
    Returns:
        tuple: (results, rate_limit_info) where rate_limit_info is a dict with
               'remaining', 'reset_time', 'limit', 'used'
    """

    if len(node_ids) > 100:
        print(f"Error: up to 100 node_ids can be inputted, but found {len(node_ids)}")
        return [None] * 100, {}
    
    url = "https://api.github.com/graphql"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # The GraphQL query using the 'nodes' field
    query = """
    query GetMultipleUsers($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on User {
          id
          login
          name
          company
          location
          url
          email
        }
      }
    }
    """

    variables = {
        "ids": node_ids
    }

    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        response = requests.post(
            url, 
            json={'query': query, 'variables': variables}, 
            headers=headers
        )

        # Extract rate limit info from headers
        rate_limit_info = {
            'remaining': int(response.headers.get('X-RateLimit-Remaining', 0)),
            'reset_time': int(response.headers.get('X-RateLimit-Reset', 0)),
            'limit': int(response.headers.get('X-RateLimit-Limit', 5000)),
            'used': int(response.headers.get('X-RateLimit-Used', 0))
        }

        # Handle rate limit errors (403 or 429)
        if response.status_code == 403 or response.status_code == 429:
            reset_time = rate_limit_info['reset_time']
            current_time = int(time.time())
            wait_time = max(reset_time - current_time + 5, 60)  # Wait until reset + 5s buffer, min 60s
            
            if attempt < max_retries - 1:
                print(f"Rate limit exceeded. Waiting {wait_time} seconds until reset...")
                time.sleep(wait_time)
                continue
            else:
                response.raise_for_status()

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                # Check if errors are rate limit related
                error_messages = [err.get('message', '') for err in data.get('errors', [])]
                if any('rate limit' in msg.lower() for msg in error_messages):
                    if attempt < max_retries - 1:
                        reset_time = rate_limit_info['reset_time']
                        current_time = int(time.time())
                        wait_time = max(reset_time - current_time + 5, 60)
                        print(f"Rate limit error in GraphQL response. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                
                print("Errors returned from GraphQL:", data["errors"])
            
            # Get the results (may include None values)
            nodes = data['data']['nodes']
            
            # Add the original node_id to each result
            # Results are returned in the same order as input IDs
            results = []
            for i, node in enumerate(nodes):
                if node is not None:
                    # Add the original node_id to the result
                    node['primary_github_user_id'] = node_ids[i]
                    results.append(node)
                else:
                    # Optionally, you could create a dict with just the node_id for None results
                    results.append({
                        'id': None,
                        'login': None,
                        'name': None,
                        'company': None,
                        'location': None,
                        'url': None,
                        'email': None,
                        'primary_github_user_id': node_ids[i]
                        })
            
            return results, rate_limit_info
        else:
            # For other errors, raise immediately
            response.raise_for_status()
    
    # If we've exhausted retries, raise the last response
    response.raise_for_status()