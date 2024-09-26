
import asyncio
import aiohttp
import sqlite3
import xml.etree.ElementTree as ET
import multiprocessing
import time
from functools import partial

# Database setup
def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS creative_commons_feeds
                      (id INTEGER PRIMARY KEY, url TEXT, title TEXT, lastUpdate INTEGER,
                      link TEXT, lastHttpStatus INTEGER, dead INTEGER, contentType TEXT,
                      itunesId INTEGER, originalUrl TEXT, itunesAuthor TEXT,
                      itunesOwnerName TEXT, explicit INTEGER, imageUrl TEXT,
                      itunesType TEXT, generator TEXT, newestItemPubdate INTEGER,
                      language TEXT, oldestItemPubdate INTEGER, episodeCount INTEGER,
                      popularityScore INTEGER, priority INTEGER, createdOn INTEGER,
                      updateFrequency INTEGER, chash TEXT, host TEXT,
                      newestEnclosureUrl TEXT, podcastGuid TEXT, description TEXT,
                      category1 TEXT, category2 TEXT, category3 TEXT, category4 TEXT,
                      category5 TEXT, category6 TEXT, category7 TEXT, category8 TEXT,
                      category9 TEXT, category10 TEXT, newestEnclosureDuration INTEGER,
                      copyright TEXT)''')  # Added copyright column
    conn.commit()

# Helper function to check if a copyright is Creative Commons
def is_creative_commons(copyright_text):
    if not copyright_text:
        return False
    cc_keywords = ['\bcc\b', 'cc-by', 'creative commons']
    lower_copyright = copyright_text.lower()
    return any(keyword in lower_copyright for keyword in cc_keywords) and not lower_copyright.startswith('mcc')

# Asynchronous function to fetch and process a single RSS feed
async def process_feed(session, url, row):
    try:
        async with session.get(url, timeout=30) as response:
            if response.status == 200:
                content = await response.text()
                root = ET.fromstring(content)
                copyright_element = root.find('.//copyright')
                if copyright_element is not None and is_creative_commons(copyright_element.text):
                    # Create a new row with the copyright information
                    new_row = list(row) + [copyright_element.text]
                    return new_row
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
    return None

# Function to process a chunk of data
async def process_chunk(chunk):
    async with aiohttp.ClientSession() as session:
        tasks = [process_feed(session, row[1], row) for row in chunk]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if r is not None and not isinstance(r, Exception)]

# Function to run in each process
def run_chunk_process(chunk):
    try:
        return asyncio.run(process_chunk(chunk))
    except Exception as e:
        print(f"Error in process: {str(e)}")
        return []

# Function to handle a single chunk with retries
def process_chunk_with_retries(chunk, max_retries=3):
    for attempt in range(max_retries):
        try:
            return run_chunk_process(chunk)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to process chunk after {max_retries} attempts: {str(e)}")
                return []
            time.sleep(1)  # Wait before retrying

# Main function to orchestrate the processing
def main(input_db, output_db, chunk_size=100):
    input_conn = create_connection(input_db)
    output_conn = create_connection(output_db)
    create_table(output_conn)

    cursor = input_conn.cursor()
    cursor.execute("SELECT * FROM podcasts LIMIT 100")

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    try:
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            
            results = pool.apply_async(process_chunk_with_retries, (chunk,)).get()
            
            if results:
                output_cursor = output_conn.cursor()
                output_cursor.executemany('''INSERT INTO creative_commons_feeds VALUES
                                             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                              ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', results)
                output_conn.commit()
    except Exception as e:
        print(f"An error occurred in the main process: {str(e)}")
    finally:
        pool.close()
        pool.join()
        input_conn.close()
        output_conn.close()

if __name__ == "__main__":
    main("podcastindex_feeds.db", "CC_database_1.db")
# %%
