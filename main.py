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

#Installing Competitions.csv
df = pd.read_csv('Competitions.csv')
#Filter rows where id == 17592
competition_id = df[df['Slug'] == 'connectx']['Id']
#Load the episodes dataset
episodes_df = pd.read_csv('Episodes.csv')
#Set the min top players with Score >3000
LOWEST_SCORE_THRESH = 3000.0
#Load the EpisodeAgents dataset
high_score_episode_ids = set()
chunksize = 500_000

for chunk in pd.read_csv('EpisodeAgents.csv', chunksize=chunksize,
                         dtype={'EpisodeId': 'int32', 'UpdatedScore': 'float32'}):
    high_score_chunk = chunk[chunk['UpdatedScore'] > LOWEST_SCORE_THRESH]
    high_score_episode_ids.update(high_score_chunk['EpisodeId'].unique())
#Filter rows where competitionId == 17592
filtered_episodes = episodes_df[episodes_df['CompetitionId'] == competition_id.item()]
#Create Data Frame with the episodes >3000
filtered_episodes_df = filtered_episodes[filtered_episodes['Id'].isin(high_score_episode_ids)]
#save into CSV
filtered_episodes_df.to_csv('FilteredEpisodes_UpdatedScoreAbove3000.csv', index=False)
#Get the list of episode IDs
EpisodeId = filtered_episodes['Id'].tolist()
#Create output_folder for episodes results
OUTPUT_DIR = Path("Episodes_output")
#Create DataBase
DB_PATH = "downloaded_episodes_id.db"
#Set boto3
s3 = boto3.client('s3')
#Set Bucket on s3
BUCKET = "connectx-storage-37012"
#Set Folder name on S3
S3_PREFIX = "Episodes_output/"
#new folder for ZIP archives
ARCHIVE_PREFIX = "archives/"
#files per ZIP
ARCHIVE_SIZE = 100
#Path to db which is located on S3
DB_S3_PATH = "s3://connectx-storage-37012/downloaded_episodes_id.db"
EPISODE_LIMIT_SIZE = 50

#Download updated DB from S3
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
#Get the list of all downloaded ids' from DB
def load_downloaded_ids(cursor):
    cursor.execute("SELECT episodeId FROM downloaded")
    return {str(row[0]) for row in cursor.fetchall()}

# ---------------- S3 helpers ----------------
#Return list of all S3 keys of archives
def list_archive_keys_all():
    archives = []
    kwargs = {"Bucket": BUCKET, "Prefix": S3_PREFIX}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents") or []
        for obj in contents:
            key = obj["Key"]
            if key.endswith(".zip"):
                archives.append(key)
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp.get("NextContinuationToken")
            continue
        break
    #Sort by numbers in name (Episodes_output/{n}.zip)
    def key_to_num(k):
        try:
            name = k.replace(S3_PREFIX, "")
            return int(name.replace(".zip", ""))
        except:
            return 0
    archives.sort(key=key_to_num)
    return archives
#Download zip-archive and return dict. Return empty is there is no archive
def download_archive_to_dict(s3_key):
    buf = io.BytesIO()
    try:
        s3.download_fileobj(BUCKET, s3_key, buf)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return {}
        raise
    buf.seek(0)
    files = {}
    with zipfile.ZipFile(buf, "r") as z:
        for name in z.namelist():
            files[name] = z.read(name)
    return files

# ---------------- Core: upload current_batch into proper archives ----------------
current_batch = []  # list of (filename, bytes)

#Add already-downloaded files to DB
def sync_local_files_with_db(cursor, downloaded_id: set):
    downloaded_files = {f for f in os.listdir(OUTPUT_DIR)
                    if f.endswith('.json')}
    for filename in downloaded_files:
        try:
            existing_episode_id = int(os.path.splitext(filename)[0])
            filepath = OUTPUT_DIR / filename
            file_timestamp = datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(timespec='seconds')

            #Insert into DB if not already there
            cursor.execute("""
                INSERT OR IGNORE INTO downloaded (episodeId, date, local_path)
                VALUES (?, ?, ?)
            """, (existing_episode_id, file_timestamp, str(filepath)))

            # Add to set of downloaded IDs
            downloaded_id.add(str(existing_episode_id))
        except ValueError:
            #Skip files with unexpected names (e.g. not numeric)
            print(f"Skipping file '{filename}' (invalid episode ID format)")
    return downloaded_id
#List of downloaded id's End-----------------------------------------

#Function for  saving the episodes results in json format
def save_episode(episode_id, conn, cursor, downloaded_id):
    global current_batch
    url = f"https://www.kaggleusercontent.com/episodes/{episode_id}.json"
    r = requests.get(url)
    if r.status_code != 200:
        print(f"Request error for episode {episode_id}: status {r.status_code}")
        return
    try:
        bytes_content = json.dumps(r.json()).encode("utf-8")
        fname = f"{episode_id}.json"
        #Avoid duplication
        if fname not in {n for n, _ in current_batch}:
            current_batch.append((fname, bytes_content))
            downloaded_id.add(str(episode_id))
            # If locally >= ARCHIVE_SIZE, to record
            if len(current_batch) >= ARCHIVE_SIZE:
                upload_current_batch(conn, cursor)
    except Exception as e:
        print(f"JSON decode error for episode {episode_id}: {e}")
#Input file in 1-st available batch, upload DB
def upload_current_batch(conn, cursor):
    global current_batch

    if not current_batch:
        return

    #Get the list of archives in order
    archives = list_archive_keys_all()

    #Find the index of first semi-full archive
    target_idx = None
    for i, key in enumerate(archives):
        cnt = get_archive_filecount(key)
        if cnt < ARCHIVE_SIZE:
            target_idx = i
            break

    #If there is no semi-full - start with the new (number = len(archives)+1)
    if target_idx is None:
        target_num = len(archives) + 1
    else:
        # извлечь номер архива из key
        key = archives[target_idx]
        num_str = key.replace(S3_PREFIX, "").replace(".zip", "")
        try:
            target_num = int(num_str)
        except:
            target_num = len(archives) + 1

    # Fill the batch
    while current_batch:
        zip_filename = f"{target_num}.zip"
        s3_key = f"{S3_PREFIX}{zip_filename}"

        #Download existing files
        existing_files = download_archive_to_dict(s3_key)
        existing_count = len(existing_files)

        merged = list(existing_files.items())
        #Add current_batch items
        merged_names = {name for name, _ in merged}
        for fname, content in current_batch:
            if fname in merged_names:
                #Update in merged
                for idx, (n, c) in enumerate(merged):
                    if n == fname:
                        merged[idx] = (fname, content)
                        break
            else:
                merged.append((fname, content))
                merged_names.add(fname)

        # If merged <= ARCHIVE_SIZE — archive is full and exit
        if len(merged) <= ARCHIVE_SIZE:
            to_write = merged
            remainder = []
        else:
            to_write = merged[:ARCHIVE_SIZE]
            remainder = merged[ARCHIVE_SIZE:]

        #Create zip с to_write
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for fname, content in to_write:
                z.writestr(fname, content)
        buf.seek(0)

        #Upload on S3
        s3.upload_fileobj(buf, BUCKET, s3_key)
        print(f"Uploaded/Updated {zip_filename}: now contains {len(to_write)} files")

        #Update DB with files in to_write
        date_now = datetime.now().isoformat(timespec="seconds")
        for fname, _ in to_write:
            episode_id = int(fname.replace(".json", ""))
            s3_full_path = f"s3://{BUCKET}/{S3_PREFIX}{zip_filename}/{fname}"
            cursor.execute("""
                INSERT OR REPLACE INTO downloaded (episodeId, date, local_path)
                VALUES (?, ?, ?)
            """, (episode_id, date_now, s3_full_path))
        conn.commit()

        #Prepare new current_batch from remainder (rest files)
        current_batch = [(fname, content) for fname, content in remainder]

        #Go to new archive
        target_num += 1

def get_archive_filecount(s3_key):
    buf = io.BytesIO()
    try:
        s3.download_fileobj(BUCKET, s3_key, buf)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return 0
        raise
    buf.seek(0)
    with zipfile.ZipFile(buf, "r") as z:
        return len(z.namelist())

#Upload updated DB back to S3
def upload_db_to_s3():
    print("Uploading updated DB to S3...")
    subprocess.run(["aws", "s3", "cp", DB_PATH, DB_S3_PATH], check=True)
    print("DB uploaded successfully.")

#def remove_local_entries(cursor, output_dir="Episodes_output"):
    #pattern = "s3://connectx-storage-%"
    #cursor.execute("DELETE FROM downloaded WHERE local_path LIKE ?", (pattern,))

#Transform EpisodeId to str and filter for top score > 3000.0 to compare with downloaded_id
episode_ids_all = [str(eid) for eid in EpisodeId if eid in high_score_episode_ids]

download_db_from_s3()

conn, cursor = init_db()
downloaded_id = load_downloaded_ids(cursor)
conn.commit()

#downloaded_id = sync_local_files_with_db(cursor, downloaded_id)
#conn.commit()

#Comparing episode_ids_all with downloaded_id
remaining_ids = [eid for eid in episode_ids_all if eid not in downloaded_id]

#Limit download size to EPISODE_LIMIT_SIZE
to_download = remaining_ids[:EPISODE_LIMIT_SIZE]

#Launch and Save the first 7 episodes
for eid in to_download:
    #Skip if episode is in DB
    if str(eid) in downloaded_id:
        print(f"Episode {eid} already exists in S3. Skipping.")
        continue
    save_episode(int(eid),conn, cursor,downloaded_id)

#Upload rest episodes in current batch
if current_batch:
    upload_current_batch(conn, cursor)

#remove_local_entries(cursor)
#conn.commit()

#Close connection
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