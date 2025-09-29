import pandas as pd
import requests as requests
import json
from pathlib import Path
import os

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

# Create outpu_folder for episodes results
OUTPUT_DIR = Path("Episodes_output")

LOWEST_SCORE_THRESH = 2000
EPISODE_LIMIT_SIZE = 15

# Get the list of ids' of all files from folder Episodes_output
downloaded_files = {f for f in os.listdir(OUTPUT_DIR)
                    if f.endswith('.json')}
downloaded_id = {os.path.splitext(f)[0] for f in downloaded_files}


# Function for  saving the episodes results in json format
def save_episode(episode_id: int):
    # Duplicates verification
    filepath = OUTPUT_DIR / f"{episode_id}.json"
    if filepath.exists():
        print(f"Episode {episode_id} already downloaded. Skipping.")
        return

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

            # Append new ids in the downloaded list of episodes
            downloaded_id.add(str(episode_id))

        except Exception as e:
            print(f"JSON decode error for episode {episode_id}: {e}")
    else:
        print(f"Request error for episode {episode_id}: status {re.status_code}")


# Launch and Save the first 15 episodes
for eid in EpisodeId[:EPISODE_LIMIT_SIZE]:
    save_episode(eid)
