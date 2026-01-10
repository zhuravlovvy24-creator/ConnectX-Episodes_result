#files per ZIP
from pathlib import Path

ARCHIVE_SIZE = 100
BUCKET = "connectx-storage-37012"
#Set Folder name on S3
S3_PREFIX = "Episodes_output/"
#new folder for ZIP archives
ARCHIVE_PREFIX = "archives/"

#Create local DataBase
DB_PATH = "downloaded_episodes_id.db"
#Path to db which is located on S3
DB_S3_PATH = f"s3://{BUCKET}/{DB_PATH}"
DB_S3_KEY = f"{DB_PATH}"

EPISODE_LIMIT_SIZE = 200
# Set the min top players with Score >3000
LOWEST_SCORE_THRESH = 3000.0
chunksize = 500_000

#Create output_folder for episodes results
OUTPUT_DIR = Path("Episodes_output")

