import os
import pandas as pd
import logging
import datetime
from subprocess import check_output, STDOUT
import threading

def now():
  return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(message):
    logging.info(message)
    print(message)

movie = "Finding Dory"
movie_release_start = "2016-09-15"
movie_release_end = "2016-12-08"

# one year before the release (2016-09-15)
dt_release_start = datetime.datetime.fromisoformat(movie_release_start)
two_weeks = datetime.timedelta(14)
start = (dt_release_start - two_weeks).strftime("%Y-%m-%d")
# last release day (2016-12-08)
end = movie_release_end  # not inclusive
max_results = -1
max_results_str = f"--max-results {max_results}" if max_results >= 0 else ""
days = pd.date_range(start=start,end=end).astype(str).tolist()

logging.basicConfig(filename=f"./movie/{movie}/log.txt", encoding='utf-8', level=logging.DEBUG, format='%(message)s')

if not os.path.isdir(f"./movie/{movie}"):
    os.mkdir(f"./movie/{movie}")


def _get_next_day(day):
    dt_day = datetime.datetime.strptime(day, "%Y-%m-%d")
    dt_next_day = dt_day + datetime.timedelta(1)
    next_day = dt_next_day.strftime("%Y-%m-%d")
    return next_day


def scrapy_daily_tweets_movie(movie, day):
    next_day = _get_next_day(day)

    file = f"./movie/{movie}/{day}.jl"
    cmd = f'snscrape {max_results_str} ' \
          f'--jsonl ' \
          f'--progress ' \
          f'--since {day} ' \
          f'twitter-search "{movie} until:{next_day}" > "{file}"'
    log(f"{now()} | CMD: {cmd} - START")
    try:
        check_output(cmd, stderr=STDOUT, shell=True, universal_newlines=True)
        log(f"{now()} | CMD: {cmd} - END")
    except Exception as e:
        log(f"{now()} | CMD: {cmd} - FAIL")
        log(f"{now()} | ERROR: {cmd}\n {e.output}")

began_at = datetime.datetime.now()
log(f"{now()} | MOVIE: {movie} - START")
threads = list()

for i in range(len(days)-1):
    t = threading.Thread(target=scrapy_daily_tweets_movie, args=(movie, days[i]))
    threads.append(t)
    t.start()
    #t.join()

for index, t in enumerate(threads):
    t.join()

finished_at = datetime.datetime.now()
delta = finished_at - began_at
total = str(delta)[:-7]

log(f"{now()} | MOVIE: {movie} - END")
log(f"TOTAL: {total}")
