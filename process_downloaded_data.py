import os
import gzip, json
import requests

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
                self.chunk.append(data)
        except:
            print "error storing chunk \n"
            print line
            raise

    def iter_files(self):
        self.output_file = open(os.path.join(self.path, 'output.txt'), 'w+')
        file_generator = self.all_files()

        for f in file_generator:
            gfile = gzip.open('./'+f)
            for line in gfile:
                self.process_line(line)
            gfile.close()
        self.output_file.close()

# class SMTASGnipDataProcessor(GnipDataProcessor):
#     def process_chunk(self):
#         url = "http://localhost:8000/drivers/gnip/"
#         r = requests.post(url, data={"data" : json.dumps(chunk)})

# smtas = SMTASGnipDataProcessor('./hurricane2012', chunk_size=100)
# smtas.iter_files()

class GeoCSVGnipDataProcessor(GnipDataProcessor):
    def process_chunk(self):
        # this is where we process the loaded json and write to a csv
        for item in self.chunk:
            if 'geo' in item:
                posted_time = item['postedTime']
                geo = ", ".join([str(i) for i in item['geo']['coordinates']])
                self.output_file.write("%s\t%s\n" % (posted_time, geo))


if __name__ == '__main__':
    csv = GeoCSVGnipDataProcessor('./spanish_march/', chunk_size=50)
    csv.iter_files()
