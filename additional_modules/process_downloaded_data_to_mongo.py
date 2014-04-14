import os
import gzip, json
import requests
from pymongo import MongoClient

class GnipDataProcessor(object):

    def __init__(self, path, chunk_size=50):
        self.path = path
        self.chunk = []
        self.chunk_size = chunk_size

    def all_files(self):
        for path, dirs, files in os.walk(self.path):
            for f in files:
                yield os.path.join(path, f)

    def process_chunk(self):
        raise NotImplementedError

    def process_line(self, line):
        try:
            if len(self.chunk) > self.chunk_size:
                self.process_chunk()
                self.chunk = []
            if line.strip() != "":
                data = json.loads(line)
                if 'geo' in data:
                    self.chunk.append(data)
        except:
            print "error storing chunk \n"
            print line
            raise

    def iter_files(self):
        file_generator = self.all_files()

        for f in file_generator:
            print f
            gfile = gzip.open('./'+f)
            for line in gfile:
                self.process_line(line)
            gfile.close()

class GeoCSVGnipDataProcessor(GnipDataProcessor):
    def process_chunk(self):
        # this is where we process the loaded json and write to a csv
        tw_spanish.insert(self.chunk)

if __name__ == '__main__':
    client = MongoClient()
    db = client.twitter_test
    tw_spanish = db.spanish_tweets

    csv = GeoCSVGnipDataProcessor('./spanish_march/', chunk_size=50)
    csv.iter_files()
