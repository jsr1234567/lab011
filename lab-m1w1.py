import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb

POKEAPI_URL = "https://pokeapi.co/api/v2/pokemon"
BATCH_SIZE = 20

import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

POKEAPI_URL = "https://pokeapi.co/api/v2/pokemon"
BATCH_SIZE = 20

def fetch_detail(url):
    try:
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            data = res.json()
            return {
                "id": data["id"],
                "name": data["name"],
                "height": data["height"],
                "weight": data["weight"],
                "types": [t["type"]["name"] for t in data["types"]],
                "abilities": [a["ability"]["name"] for a in data["abilities"]],
                "base_experience": data["base_experience"]
            }
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None  # if request fails

def extract_data(offset=0, limit=BATCH_SIZE, output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)

    response = requests.get(f"{POKEAPI_URL}?offset={offset}&limit={limit}")
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")

    results = response.json().get("results", [])

    # Use ThreadPoolExecutor to fetch details concurrently
    detailed_data = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_detail, p["url"]): p for p in results}
        for future in as_completed(futures):
            result = future.result()
            if result:
                detailed_data.append(result)

    # Save to Parquet
    df = pd.DataFrame(detailed_data)
    file_path = os.path.join(output_dir, f"pokemon_offset_{offset}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    new_offset = offset + limit
    return file_path, new_offset



def load_data(parquet_path, db_path="pokedex.duckdb"):
    # Connect to DuckDB
    con = duckdb.connect(database=db_path)

    # Create main table if not exists
    con.execute("""
    CREATE TABLE IF NOT EXISTS pokedex (
        id INTEGER PRIMARY KEY,
        name TEXT,
        height INTEGER,
        weight INTEGER,
        types TEXT,
        abilities TEXT,
        base_experience INTEGER
    )
""")

    # Create metadata table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            file TEXT PRIMARY KEY,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    file_name = os.path.basename(parquet_path)

    # Check if this file has already been loaded
    already_loaded = con.execute(
        "SELECT COUNT(*) FROM metadata WHERE file = ?", (file_name,)
    ).fetchone()[0]

    if already_loaded:
        print(f"✅ Skipping {file_name} (already loaded)")
        con.close()
        return

    # Read the parquet file
    df = pd.read_parquet(parquet_path)

    # Insert data into pokedex
    con.register("df_view", df)
    con.execute("""
        INSERT OR REPLACE INTO pokedex
        SELECT * FROM df_view
    """)

    # Track the file in metadata
    con.execute("INSERT INTO metadata (file) VALUES (?)", (file_name,))

    print(f"📥 Loaded {file_name} into DuckDB.")

    con.close()

def transform_data(db_path="pokedex.duckdb"):
    """
    Performs aggregations on Pokémon data:
    - Counts total Pokémon.
    - Identifies the first and last Pokémon ID.
    - Stores results in the 'pokemon_stats' table.
    
    :param db_path: Path to the DuckDB database file.
    """
    
    # ✅ Step 1: Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # ✅ Step 2: Create 'pokemon_stats' table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pokemon_stats (
            total_pokemon INTEGER,
            first_pokemon_id INTEGER,
            last_pokemon_id INTEGER
        )
    """)
    
    # ✅ Step 3: Perform aggregations
    result = conn.execute("""
        SELECT
            COUNT(id) AS total_pokemon,
            MIN(id) AS first_pokemon_id,
            MAX(id) AS last_pokemon_id
        FROM pokedex
    """).fetchone()  # fetchone() gets the result as a tuple
    
    # ✅ Step 4: Insert results into pokemon_stats table
    conn.execute("""
        INSERT INTO pokemon_stats (total_pokemon, first_pokemon_id, last_pokemon_id)
        VALUES (?, ?, ?)
    """, result)  # Pass the aggregation result to the query
    
    print(f"✅ Data transformation successful: {result}")
    
    # ✅ Step 5: Close the connection
    conn.close()

def main():
    parquet_file, next_offset = extract_data(offset=0, limit=20)
    print(f"🗂️ Data saved to: {parquet_file}")
    print(f"👉 Next offset would be: {next_offset}")
    print("🎉 Data extraction completed successfully!")

    # Optionally load it into DuckDB
    load_data(parquet_file)
    print(f"🦆 Loaded into DuckDB !")
    transform_data()
    print("🥇 Transformation step finished. Data saved to pokemon_stats!")


if __name__ == "__main__":
    main()