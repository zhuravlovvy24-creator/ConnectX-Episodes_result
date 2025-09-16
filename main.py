import pandas as pd 
import requests as requests
import json
from pathlib import Path    

# Installin Competitions.csv
df = pd.read_csv('Competitions.csv')
#print(df.head())

# Filter rows where id == 17592
id = df[df['Slug'] == 'connectx']['Id']

# Display the result
#print(id)

# Load the episodes dataset
episodes_df = pd.read_csv('Episodes.csv')  # adjust the path if needed

# Filter rows where competitionId == 17592
filtered_episodes = episodes_df[episodes_df['CompetitionId'] == id.item()]

# Display the filtered episodes
#print(filtered_episodes)

# Get the list of episode IDs
EpisodId = filtered_episodes['Id'].tolist()

# Create outpu_folder for episods results
OUTPUT_DIR = Path("Episods_output")

LOWEST_SCORE_THRESH = 2000
EPISODE_LIMIT_SIZE = 10

# Function for  saving the episods results in json format
def saveEpisode(EpisodId:int):
    # Create URL for request
    GET_URL = f"https://www.kaggleusercontent.com/episodes/{EpisodId}.json"
    # request
    re = requests.get(GET_URL)
    if re.status_code == 200:
        try:
            # save replay
            replay = re.json()
            OUTPUT_DIR.mkdir(exist_ok=True)
            with open(OUTPUT_DIR / f'{EpisodId}.json', 'w') as f:
                json.dump(replay, f)
            print(f"Эпизод {EpisodId} успешно сохранён.")
        except Exception as e:
            print(f"Ошибка декодирования JSON для эпизода {EpisodId}: {e}")
    else:
        print(f"Ошибка запроса для эпизода {EpisodId}: статус {re.status_code}")

# Launch and Save the first 10 episodes  
for eid in EpisodId[:EPISODE_LIMIT_SIZE]:
    saveEpisode(eid)