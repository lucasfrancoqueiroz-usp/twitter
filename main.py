import os
import pandas as pd
import logging
import datetime
from subprocess import check_output, STDOUT
import threading
from google_drive_uploader import GoogleDriveUploader
from utils import Utils

class Extract():

    def get_next_day(day):
        dt_day = datetime.datetime.strptime(day, "%Y-%m-%d")
        dt_next_day = dt_day + datetime.timedelta(1)
        next_day = dt_next_day.strftime("%Y-%m-%d")
        return next_day


    def get_start(movie_release):
        global days_before_release
        start = (movie_release - days_before_release).strftime("%Y-%m-%d")
        return start


    def get_end(movie_release):
        global days_after_release
        one_day = datetime.timedelta(1)
        end = movie_release + days_after_release
        end = end + one_day  # this is necessary because the last day is not inclusive in the search query
        end = end.strftime("%Y-%m-%d")
        return end


    def get_movies():
        df = pd.read_csv("movies_search_dory.csv")
        info = df.apply(lambda row: (
            row['release_id'],
            row['release'],
            row['search'],
            datetime.datetime.strptime(row['release_start'], "%Y-%m-%d")
        ), axis=1).values
        return info


    def get_expected_requests(search, release_id, days):
        expected_requests = []
        for i in range(len(days) - 1):
            request = {
                'search': search,
                'release_id': release_id,
                'day': days[i]
            }
            expected_requests.append(request)
        return expected_requests


    def scrapy_tweets(search, release_id, day, successful_requests):
        next_day = Extract.get_next_day(day)
        max_results_str = f"--max-results {max_results}" if max_results >= 0 else ""

        file = f"./movies/{release_id}_{day}.jl"
        cmd = f'snscrape {max_results_str} ' \
              f'--jsonl ' \
              f'--progress ' \
              f'--since {day} ' \
              f'twitter-search "{search} until:{next_day}" > "{file}"'
        Utils.log(f"{Utils.now()} | CMD: {cmd} - START")
        try:
            check_output(cmd, stderr=STDOUT, shell=True, universal_newlines=True)
            Utils.log(f"{Utils.now()} | CMD: {cmd} - END")
            successful_requests.append({
                'search': search,
                'release_id': release_id,
                'day': day
            })
        except Exception as e:
            Utils.log(f"{Utils.now()} | CMD: {cmd} - FAIL")
            Utils.log(f"{Utils.now()} | ERROR: {cmd}\n {e.output}")


movies_folder = "./movies"
DAYS_BEFORE_RELEASE = 14
DAYS_AFTER_RELEASE = 14
days_before_release = datetime.timedelta(DAYS_BEFORE_RELEASE)
days_after_release = datetime.timedelta(DAYS_AFTER_RELEASE)
max_results = -1

if __name__ == "__main__":

    logging.basicConfig(filename=f"{movies_folder}/log.txt", encoding='utf-8', level=logging.DEBUG, format='%(message)s')
    movies = Extract.get_movies()
    c = 1
    for release_id, movie, search, release_date in movies:
        began_at = datetime.datetime.now()
        Utils.log(f"{Utils.now()} | {c} of {len(movies)}")
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - START")

        # Days range
        start = Extract.get_start(release_date)
        end = Extract.get_end(release_date)  # last day is not inclusive
        days = pd.date_range(start=start, end=end).astype(str).tolist()

        # Scrapy movie
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - DOWNLOADING START")
        successful_requests = []
        expected_requests = Extract.get_expected_requests(search, release_id, days)
        while len(successful_requests) < len(expected_requests):
            expected_requests = [r for r in expected_requests if r not in successful_requests]
            threads = list()
            for request in expected_requests:
                t = threading.Thread(target=Extract.scrapy_tweets, args=(request['search'], request['release_id'], request['day'], successful_requests))
                threads.append(t)
                t.start()
            for index, t in enumerate(threads):
                t.join()
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - DOWNLOADING END")

        # Upload files to Google Drive
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - UPLOADING START")
        GoogleDriveUploader.upload_movie_folder(f"{movies_folder}/*.jl")
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - UPLOADING END")

        # Remove from local machine
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - REMOVING START")
        Utils.remove_files(f"{movies_folder}/*.jl")
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - REMOVING END")

        finished_at = datetime.datetime.now()
        delta = finished_at - began_at
        total = str(delta)[:-7]
        Utils.log(f"{Utils.now()} | MOVIE: {movie} - END")
        Utils.log(f"TOTAL: {total}")

        c += 1