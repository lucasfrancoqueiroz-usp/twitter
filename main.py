import os
import pandas as pd
import logging
import datetime
from subprocess import check_output, STDOUT
import threading

class Utils():

    def now():
      return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def log(message):
        logging.info(message)
        print(message)


class GoogleDrive():
    def __init__(self):
        pass


class Extract():

    def create_movie_folder(movie):
        if not os.path.isdir(f"{movies_folder}{movie}"):
            os.mkdir(f"{movies_folder}{movie}")


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


    def scrapy_tweets(search, release_id, day):
        next_day = Extract.get_next_day(day)
        max_results_str = f"--max-results {max_results}" if max_results >= 0 else ""

        file = f"./movies/{release_id}/{day}.jl"
        cmd = f'snscrape {max_results_str} ' \
              f'--jsonl ' \
              f'--progress ' \
              f'--since {day} ' \
              f'twitter-search "{search} until:{next_day}" > "{file}"'
        Utils.log(f"{Utils.now()} | CMD: {cmd} - START")
        try:
            check_output(cmd, stderr=STDOUT, shell=True, universal_newlines=True)
            Utils.log(f"{Utils.now()} | CMD: {cmd} - END")
        except Exception as e:
            Utils.log(f"{Utils.now()} | CMD: {cmd} - FAIL")
            Utils.log(f"{Utils.now()} | ERROR: {cmd}\n {e.output}")


movies_folder = "./movies/"
DAYS_BEFORE_RELEASE = 14
DAYS_AFTER_RELEASE = 14
days_before_release = datetime.timedelta(DAYS_BEFORE_RELEASE)
days_after_release = datetime.timedelta(DAYS_AFTER_RELEASE)
max_results = -1

if __name__ == "__main__":
    for release_id, movie, search, release_date in [Extract.get_movies()[0]]:
        began_at = datetime.datetime.now()

        # Create local folder
        Extract.create_movie_folder(release_id)

        logging.basicConfig(filename=f"{movies_folder}{release_id}/log.txt", encoding='utf-8', level=logging.DEBUG, format='%(message)s')
        Utils.log(f"{Utils.now()} | ID: {release_id} - MOVIE: {movie} - START")

        # Scrapy movie
        start = Extract.get_start(release_date)
        end = Extract.get_end(release_date)  # last day is not inclusive
        days = pd.date_range(start=start, end=end).astype(str).tolist()
        threads = list()
        for i in range(len(days)-1):
            t = threading.Thread(target=Extract.scrapy_tweets, args=(search, release_id, days[i]))
            threads.append(t)
            t.start()
        for index, t in enumerate(threads):
            t.join()
        finished_at = datetime.datetime.now()

        # Upload files to Google Drive


        delta = finished_at - began_at
        total = str(delta)[:-7]
        Utils.log(f"{Utils.now()} | MOVIE: {movie} - END")
        Utils.log(f"TOTAL: {total}")
