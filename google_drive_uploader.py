from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth
from gd_tweets_folder import tweets_folder_id  # Google Drive folder id
from utils import Utils

import json
import glob
import os

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

class GoogleDriveUploader:

    def _create_gd_movie_folder(parent_folder_id, subfolder_name):
        '''
        How to use example
        gd_movie_folder = _create_gd_movie_folder(tweets_folder, release_id)
        print(f'Folder created: {gd_movie_folder["id"]}')
        '''

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


    def upload_movie_folder(movie_folder):
        tweet_files = glob.glob(movie_folder)
        for tweet_file in tweet_files:
            filename = os.path.basename(tweet_file)
            GoogleDriveUploader._delete_file(filename)
            Utils.log(f"{Utils.now()} | FILENAME: {filename} - UPLOADING START")
            gd_movie_file = GoogleDriveUploader._upload_gd_movie_file(filename, tweet_file, tweets_folder_id)
            Utils.log(f"{Utils.now()} | FILENAME: {filename} - UPLOADING END")


    def _delete_file(filename):
        file_list = drive.ListFile({'q': f"title = '{filename}' and trashed=False"}).GetList()
        try:
            for file in file_list:
                file.Delete()
        except:
            pass
