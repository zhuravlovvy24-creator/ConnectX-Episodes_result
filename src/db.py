import datetime
import io
import os
import sqlite3
import zipfile
from typing import Any
import src.s3
from src import consts, s3
from src.consts import BUCKET, S3_PREFIX
from datetime import datetime


#Create DB and table
def init_db():
    conn = sqlite3.connect(consts.DB_PATH)
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


# ---------------- Core: upload current_batch into proper archives ----------------
current_batch = []  # list of (filename, bytes)

#Add already-downloaded files to DB
def sync_local_files_with_db(cursor, downloaded_id: set):
    downloaded_files = {f for f in os.listdir(consts.OUTPUT_DIR)
                    if f.endswith('.json')}
    for filename in downloaded_files:
        try:
            existing_episode_id = int(os.path.splitext(filename)[0])
            filepath = consts.OUTPUT_DIR / filename
            file_timestamp = datetime.datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(timespec='seconds')

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

#Input file in 1-st available batch, upload DB
def upload_current_batch(conn, cursor):
    global current_batch

    if not current_batch:
        return

    #Get the list of archives in order
    archives = src.s3.list_archive_keys_all()

    #Find the index of first semi-full archive
    target_idx = None
    for i, key in enumerate(archives):
        cnt = src.s3.get_archive_filecount(key)
        if cnt < consts.ARCHIVE_SIZE:
            target_idx = i
            break

    #If there is no semi-full - start with the new (number = len(archives)+1)
    if target_idx is None:
        target_num = len(archives) + 1
    else:
        #extract archive number from key
        key = archives[target_idx]
        num_str = key.replace(consts.S3_PREFIX, "").replace(".zip", "")
        try:
            target_num = int(num_str)
        except:
            target_num = len(archives) + 1

    # Fill the batch
    while current_batch:
        zip_filename = f"{target_num}.zip"
        s3_key = f"{consts.S3_PREFIX}{zip_filename}"

        #Download existing files
        existing_files = src.s3.download_archive_to_dict(s3_key)
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
        if len(merged) <= consts.ARCHIVE_SIZE:
            to_write = merged
            remainder = []
        else:
            to_write = merged[:consts.ARCHIVE_SIZE]
            remainder = merged[consts.ARCHIVE_SIZE:]

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

conn, cursor = init_db()

downloaded_ids = load_downloaded_ids(cursor)

def filter_new_ids(all_filtered_ids: set, downloaded_ids: set) -> list[Any]:
    new_ids_set = all_filtered_ids - downloaded_ids
    return list(new_ids_set)


def update_db(episode_list: list[tuple[str, bytes]], archive_name: str, cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    date_now = datetime.now().isoformat(timespec="seconds")
    s3_archive_key = f"{consts.S3_PREFIX}{archive_name}"

    for episode_id, _ in episode_list:
        try:
            # Ensure ID is an integer for the DB column
            episode_id_int = int(episode_id)

            # Insert or Replace ensures we don't crash on duplicates; we just update the info
            cursor.execute("""
                    INSERT OR REPLACE INTO downloaded (episodeId, date, local_path)
                    VALUES (?, ?, ?)
                """, (episode_id_int, date_now, s3_archive_key))

        except ValueError:
            print(f"Skipping invalid episode ID: {episode_id}")

        # Commit the transaction to save changes to the file
    conn.commit()
    print(f"Database updated with {len(episode_list)} episodes in archive: {archive_name}")


def connect_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print(f"Successfully connected to database: {db_path}")
    return None