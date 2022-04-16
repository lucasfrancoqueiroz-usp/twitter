from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth

import json
import glob
import os

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

tweets_folder = "[id]"
release_id = "Finding Dory"
movie_folder = r".\movies\rl2893252097\*.jl"

def _create_gd_movie_folder(parent_folder_id, subfolder_name):
    newFolder = drive.CreateFile({
      'title': subfolder_name,
      "parents": [{
          "kind": "drive#fileLink",
          "id": parent_folder_id
      }],
      "mimeType": "application/vnd.google-apps.folder"
    })
    newFolder.Upload()
    return newFolder

def _upload_gd_movie_file(filename, file_fullname, gd_folder):
    f = drive.CreateFile({
        'title': filename,
        "parents": [{
            "kind": "drive#fileLink",
            "id": gd_folder
        }],
    })
    f.SetContentFile(file_fullname)
    f.Upload()
    return f

gd_movie_folder = _create_gd_movie_folder(tweets_folder, release_id)
print(f'Folder created: {gd_movie_folder["id"]}')
tweet_files = glob.glob(movie_folder)
for tweet_file in tweet_files:
    filename = os.path.basename(tweet_file)
    gd_movie_file = _upload_gd_movie_file(filename, tweet_file, gd_movie_folder["id"])
    print(f'Uploaded: {tweet_file} | id: {gd_movie_file["id"]}')
