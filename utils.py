import datetime
import logging
import os
import glob

class Utils():

    def now():
      return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def log(message):
        logging.info(message)
        print(message)


    def remove_files(folder):
        files = glob.glob(folder)
        for f in files:
            filename = os.path.basename(f)
            Utils.log(f"{Utils.now()} | FILENAME: {filename} - REMOVING START")
            try:
                os.remove(f)
                Utils.log(f"{Utils.now()} | FILENAME: {filename} - REMOVING END")
            except OSError as e:
                Utils.log("Error: %s : %s" % (f, e.strerror))
                Utils.log(f"{Utils.now()} | FILENAME: {filename} - REMOVING FAIL")