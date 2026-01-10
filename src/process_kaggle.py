import pandas as pd
import requests
import json
from src.consts import LOWEST_SCORE_THRESH, chunksize


# Installing Competitions.csv
df = pd.read_csv('Competitions.csv')
# Filter rows where id == 17592
competition_id = df[df['Slug'] == 'connectx']['Id']
# Load the episodes dataset
episodes_df = pd.read_csv('Episodes.csv')
# Load the EpisodeAgents dataset
high_score_episode_ids = set()

for chunk in pd.read_csv('EpisodeAgents.csv', chunksize=chunksize,
                         dtype={'EpisodeId': 'int32', 'UpdatedScore': 'float32'}):
    high_score_chunk = chunk[chunk['UpdatedScore'] > LOWEST_SCORE_THRESH]
    high_score_episode_ids.update(high_score_chunk['EpisodeId'].unique())
# Filter rows where competitionId == 17592
filtered_episodes = episodes_df[episodes_df['CompetitionId'] == competition_id.item()]
# Create Data Frame with the episodes >3000
filtered_episodes_df = filtered_episodes[filtered_episodes['Id'].isin(high_score_episode_ids)]
# save into CSV
filtered_episodes_df.to_csv('FilteredEpisodes_UpdatedScoreAbove3000.csv', index=False)

#Get the list of episode IDs
EpisodeId = filtered_episodes['Id'].tolist()

#Function for  saving the episodes results in json format
def download_episode(episode_id: str) -> tuple[str, bytes] | None:
    url = f"https://www.kaggleusercontent.com/episodes/{episode_id}.json"
    r = requests.get(url)
    if r.status_code != 200:
        print(f"Request error for episode {episode_id}: status {r.status_code}")
        return None
    try:
        # Dump the JSON data back to a string and encode it to bytes
        bytes_content = json.dumps(r.json()).encode("utf-8")

        # Return the ID and the content bytes (needed for the local batch)
        return episode_id, bytes_content

    except Exception as e:
        # This will catch JSONDecodeError if the content isn't valid JSON
        print(f"JSON decode error for episode {episode_id}: {e}")
        return None


#Transform EpisodeId to str and filter for top score > 3000.0 to compare with downloaded_id
episode_ids_all = [str(eid) for eid in EpisodeId if eid in high_score_episode_ids]

def extract_ids(dataframe: pd.DataFrame) -> set:
    Id_Column = "Id"
    #Exctract column "Id"
    id_set = set(dataframe[Id_Column].astype(str).tolist())
    return id_set
