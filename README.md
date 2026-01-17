# ConnectX-Episodes_result
**What this project does**

This tool automatically collects and stores high-level game data from Kaggle.

In simple terms:
- It looks for game episodes where players have a score above 3000.
- It zips these files to save space.
- It uploads the ZIP files to AWS S3 (cloud storage).
- It uses a local database to remember what it has already downloaded so it doesn't do the same work twice.

**Quick Start Guide**

1. Setup

Make sure you have Python installed and your AWS and Kaggle keys ready on your computer.

2. Install

Open your terminal and run these commands to get the code and the necessary tools:

Bash

git clone https://github.com/zhuravlovvy24-creator/ConnectX-Episodes_result.git

cd ConnectX-Episodes_result

pip install -r requirements.txt

3. Configure

Open src/consts.py and make sure your S3 Bucket name is correct:

Python

BUCKET = "your-actual-s3-bucket-name"

4. Run

Start the process by running:

Bash

python main.py

**Tools Used**

Python – The main programming language.

SQLite – A small database to track progress.

AWS S3 (Boto3) – Cloud storage for your files.

Pandas – To filter and read the game data.