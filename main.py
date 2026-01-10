import io
import zipfile
import pandas as pd
import uuid
from src import consts, s3, db
from src.consts import BUCKET
from src.db import downloaded_ids
from src.process_kaggle import download_episode, extract_ids


def create_archive(episode_list: list[tuple[str, bytes]]) -> tuple[str, io.BytesIO]:
   archive_name = f"archive-{uuid.uuid4()}.zip"
   #create zip archive here
   buf = io.BytesIO()
   with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
       for episode_id, content in episode_list:
           json_filename = f"{episode_id}.json"
           z.writestr(json_filename, content)

   buf.seek(0)
   return archive_name, buf

def main():
    db_path: str = consts.DB_PATH
    df = pd.read_csv('FilteredEpisodes_UpdatedScoreAbove3000.csv')

    all_filtered_ids = extract_ids(df)

    temp_db_path: str | None = s3.download_db_from_s3(db_path)

    if temp_db_path is None:
        # 2a. DB NOT FOUND: db_path remains consts.DB_PATH (a string)
        conn, cursor = db.init_db()
    else:
        # 2b. DB FOUND: db_path is updated to temp_db_path (a string)
        db_path = temp_db_path
        result = db.connect_db(db_path)
        conn, cursor = result[0], result[1] if result else (None, None)  # Defensive unpacking

    new_ids = db.filter_new_ids(all_filtered_ids, downloaded_ids)

    counter = 0
    cur_episode_list: list[tuple[str, bytes]] = []
    for episode_id in new_ids[:consts.EPISODE_LIMIT_SIZE]:

        # ASSUMPTION: download_episode has been fixed to return a tuple or None.
        download_result = download_episode(episode_id)

        if download_result:
            cur_episode_list.append(download_result)

        counter += 1
        if counter == consts.ARCHIVE_SIZE:
            # unique archive name
            archive_name, archive_buffer = create_archive(cur_episode_list)
            s3_key = f"{consts.S3_PREFIX}{archive_name}"
            s3.upload_to_s3(file_buffer=archive_buffer,bucket_name=BUCKET,s3_key=s3_key)
            db.update_db(cur_episode_list, archive_name, cursor,conn)
            s3.upload_local_file_to_s3(db_path, BUCKET, consts.DB_PATH)
            cur_episode_list: list[tuple[str, bytes]] = []
    # Close the database connection ONLY after everything is done
    conn.close()
    print("Processing complete and connection closed.")

print("Processing complete and connection closed.")
if __name__ == '__main__':  # this file was called directly
    main()
