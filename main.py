import pandas as pd
import requests as requests
import json
from pathlib import Path
import os
import sqlite3
from datetime import datetime

# Installing Competitions.csv
df = pd.read_csv('Competitions.csv')
#print(df.head())

# Filter rows where id == 17592
competition_id = df[df['Slug'] == 'connectx']['Id']

# Load the episodes dataset
episodes_df = pd.read_csv('Episodes.csv')  # adjust the path if needed

# Filter rows where competitionId == 17592
filtered_episodes = episodes_df[episodes_df['CompetitionId'] == competition_id.item()]

# Get the list of episode IDs
EpisodeId = filtered_episodes['Id'].tolist()

# Create output_folder for episodes results
OUTPUT_DIR = Path("Episodes_output")

#Create DataBase
DB_PATH = "downloaded_episodes_id.db"

#Create DB and table
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

LOWEST_SCORE_THRESH = 2000
EPISODE_LIMIT_SIZE = 15

#List of downloaded id's Beginning-----------------------------------
# Get the list of ids' of all files from folder Episodes_output
downloaded_files = {f for f in os.listdir(OUTPUT_DIR)
                    if f.endswith('.json')}
downloaded_id = {os.path.splitext(f)[0] for f in downloaded_files}
#List of downloaded id's End-----------------------------------------

# Function for  saving the episodes results in json format
def save_episode(episode_id: int):
    # Create URL for request
    get_url = f"https://www.kaggleusercontent.com/episodes/{episode_id}.json"
    # request
    re = requests.get(get_url)
    if re.status_code == 200:
        try:
            # save replay
            replay = re.json()
            OUTPUT_DIR.mkdir(exist_ok=True)
            with open(OUTPUT_DIR / f'{episode_id}.json', 'w') as f:
                json.dump(replay, f)
            print(f"Episode {episode_id} successfully saved.")

            # Insert record into SQLite DB
            # -----------------------------
            date_now = datetime.now().isoformat(timespec='seconds')
            cursor.execute("""
                           INSERT
                           OR IGNORE INTO downloaded (episodeId, date, local_path)
                           VALUES (?, ?, ?)
                           """, (episode_id, date_now, str(filepath)))
            conn.commit()
            # -----------------------------

            # Append new ids in the downloaded list of episodes
            downloaded_id.add(str(episode_id))
        except Exception as e:
            print(f"JSON decode error for episode {episode_id}: {e}")
    else:
        print(f"Request error for episode {episode_id}: status {re.status_code}")

# Transform EpisodeId to str to compare with downloaded_id
episode_ids_all = [str(eid) for eid in EpisodeId]

# Comparing episode_ids_all with downloaded_id
remaining_ids = [eid for eid in episode_ids_all if eid not in downloaded_id]

# Limit download size to EPISODE_LIMIT_SIZE
to_download = remaining_ids[:EPISODE_LIMIT_SIZE]

# Function that input all episodes_ids and return filtered those that has to be downloaded
def get_new_episodes_id(all_ids: str):
    #Transform EpisodeId to str to compare with downloaded_id
    all_ids_str = [str(eid) for eid in all_ids]
    #Compare all_ids with downloaded_id
    new_ids = [eid for eid in all_ids_str if eid not in downloaded_id]
    return new_ids

# Launch and Save the first 15 episodes
for eid in to_download:
    # Duplicates verification
    filepath = OUTPUT_DIR / f"{eid}.json"
    if filepath.exists():
        print(f"Episode {eid} already downloaded. Skipping.")
        continue # skip function launch
    save_episode(int(eid))

# Transform Set downloaded_id into DataFrame
downloaded_id_df = pd.DataFrame({"Downloaded_Episode_id": list(downloaded_id)})

# Save into csv
downloaded_id_df.to_csv("Downloaded_Episodes_id.csv", index=False)

# Close connection
conn.close()