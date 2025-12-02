from os.path import exists
import botocore
import pandas as pd
import requests as requests
import json
from pathlib import Path
import os
import sqlite3
from datetime import datetime
import subprocess
import boto3
import io
import zipfile
from botocore.exceptions import ClientError

# Installing Competitions.csv
df = pd.read_csv('Competitions.csv')
# Filter rows where id == 17592
competition_id = df[df['Slug'] == 'connectx']['Id']
# Load the episodes dataset
episodes_df = pd.read_csv('Episodes.csv')  # adjust the path if needed
#Set the min top players with Score >3000
LOWEST_SCORE_THRESH = 3000.0
#Load the EpisodeAgents dataset
high_score_episode_ids = set()
chunksize = 500_000

for chunk in pd.read_csv('EpisodeAgents.csv', chunksize=chunksize,
                         dtype={'EpisodeId': 'int32', 'UpdatedScore': 'float32'}):
    high_score_chunk = chunk[chunk['UpdatedScore'] > LOWEST_SCORE_THRESH]
    high_score_episode_ids.update(high_score_chunk['EpisodeId'].unique())
# Filter rows where competitionId == 17592
filtered_episodes = episodes_df[episodes_df['CompetitionId'] == competition_id.item()]
#Create Data Frame with the episodes >3000
filtered_episodes_df = filtered_episodes[filtered_episodes['Id'].isin(high_score_episode_ids)]
# save into CSV
filtered_episodes_df.to_csv('FilteredEpisodes_UpdatedScoreAbove3000.csv', index=False)
# Get the list of episode IDs
EpisodeId = filtered_episodes['Id'].tolist()
# Create output_folder for episodes results
OUTPUT_DIR = Path("Episodes_output")
#Create DataBase
DB_PATH = "downloaded_episodes_id.db"
#Set boto3
s3 = boto3.client('s3')
#Set Bucket on s3
BUCKET = "connectx-storage-37012"
#Set Folder name on S3
S3_PREFIX = "Episodes_output/"
# new folder for ZIP archives
ARCHIVE_PREFIX = "archives/"
# files per ZIP
ARCHIVE_SIZE = 100
# Path to db which is located on S3
DB_S3_PATH = "s3://connectx-storage-37012/downloaded_episodes_id.db"
EPISODE_LIMIT_SIZE = 101

# Download updated DB from S3
def download_db_from_s3():
    print("Downloading DB from S3...")
    subprocess.run(["aws", "s3", "cp", DB_S3_PATH, DB_PATH], check=True)
    print("DB downloaded successfully.")

#Create DB and table
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS downloaded (
        episodeId INTEGER PRIMARY KEY,
        date TEXT,
        local_path TEXT
    )
    """)
    conn.commit()
    return conn, cursor

#List of downloaded id's Beginning-----------------------------------
# Get the list of all downloaded ids' from DB
def load_downloaded_ids(cursor):
    cursor.execute("SELECT episodeId FROM downloaded")
    return {str(row[0]) for row in cursor.fetchall()}

#Add already-downloaded files to DB
def sync_local_files_with_db(cursor, downloaded_id: set):
    downloaded_files = {f for f in os.listdir(OUTPUT_DIR)
                    if f.endswith('.json')}
    for filename in downloaded_files:
        try:
            existing_episode_id = int(os.path.splitext(filename)[0])
            filepath = OUTPUT_DIR / filename
            file_timestamp = datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(timespec='seconds')

            # Insert into DB if not already there
            cursor.execute("""
                INSERT OR IGNORE INTO downloaded (episodeId, date, local_path)
                VALUES (?, ?, ?)
            """, (existing_episode_id, file_timestamp, str(filepath)))

            # Add to set of downloaded IDs
            downloaded_id.add(str(existing_episode_id))
        except ValueError:
            # Skip files with unexpected names (e.g. not numeric)
            print(f"Skipping file '{filename}' (invalid episode ID format)")
    return downloaded_id
#List of downloaded id's End-----------------------------------------

#Preparation for ZIP upload
archive_number = 1
current_batch = []
#Get the latest archive in S3
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=S3_PREFIX)
existing_archives = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.zip')]
if existing_archives:
    archive_number = max(int(k.replace(S3_PREFIX, '').replace('.zip','')) for k in existing_archives) + 1
else:
    archive_number = 1


# Function for  saving the episodes results in json format
def save_episode(episode_id: int):
    global current_batch, archive_number

    # Create URL for request
    get_url = f"https://www.kaggleusercontent.com/episodes/{episode_id}.json"
    # request
    re = requests.get(get_url)
    if re.status_code == 200:
        try:
            # save replay
            replay = re.json()
            json_bytes = json.dumps(replay).encode('utf-8')
            current_batch.append((f"{episode_id}.json", json_bytes))
            # Insert record into SQLite DB
            # -----------------------------
            date_now = datetime.now().isoformat(timespec='seconds')
            cursor.execute("""
                           INSERT
                           OR IGNORE INTO downloaded (episodeId, date, local_path)
                           VALUES (?, ?, ?)
                           """, (episode_id, date_now, f"in-zip-{episode_id}.json"))
            conn.commit()
            # -----------------------------
            # Append new ids in the downloaded list of episodes
            downloaded_id.add(str(episode_id))

            # if 100 episodes were downloaded - create ZIP and upload it
            if len(current_batch) >= ARCHIVE_SIZE:
                upload_current_batch()

        except Exception as e:
            print(f"JSON decode error for episode {episode_id}: {e}")
    else:
        print(f"Request error for episode {episode_id}: status {re.status_code}")

def upload_current_batch():
    global current_batch, archive_number

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for filename, json_bytes in current_batch:
            zipf.writestr(filename, json_bytes)

    zip_buffer.seek(0)
    s3_key = f"{S3_PREFIX}{archive_number}.zip"
    s3.upload_fileobj(zip_buffer, BUCKET, s3_key)
    print(f"Uploaded ZIP: {s3_key} ({len(current_batch)} episodes)")

    archive_number += 1
    current_batch = []

#Upload updated DB back to S3
def upload_db_to_s3():
    print("Uploading updated DB to S3...")
    subprocess.run(["aws", "s3", "cp", DB_PATH, DB_S3_PATH], check=True)
    print("DB uploaded successfully.")

#def remove_local_entries(cursor, output_dir="Episodes_output"):
    #pattern = f"{output_dir}%"
    #cursor.execute("DELETE FROM downloaded WHERE local_path LIKE ?", (pattern,))

# Transform EpisodeId to str and filter for top score > 3000.0 to compare with downloaded_id
episode_ids_all = [str(eid) for eid in EpisodeId if eid in high_score_episode_ids]

download_db_from_s3()

conn, cursor = init_db()
downloaded_id = load_downloaded_ids(cursor)
conn.commit()

#downloaded_id = sync_local_files_with_db(cursor, downloaded_id)
#conn.commit()

# Comparing episode_ids_all with downloaded_id
remaining_ids = [eid for eid in episode_ids_all if eid not in downloaded_id]

# Limit download size to EPISODE_LIMIT_SIZE
to_download = remaining_ids[:EPISODE_LIMIT_SIZE]

# Launch and Save the first 7 episodes
for eid in to_download:
    #Skip if episode is in DB
    if str(eid) in downloaded_id:
        print(f"Episode {eid} already exists in S3. Skipping.")
        continue
    save_episode(int(eid))

#Upload rest episodes in current batch
if current_batch:
        upload_current_batch()

# Close connection
conn.close()
upload_db_to_s3()
#Hidden_____________
# Function that input all episodes_ids and return filtered those that has to be downloaded
#def get_new_episodes_id(all_ids: str):
    #Transform EpisodeId to str to compare with downloaded_id
    #all_ids_str = [str(eid) for eid in all_ids]
    #Compare all_ids with downloaded_id
    #new_ids = [eid for eid in all_ids_str if eid not in downloaded_id]
    #return new_ids