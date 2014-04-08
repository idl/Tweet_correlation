import os
import gzip, json



top_users = {}

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
        except Exception as e:
            print "error storing chunk \n"
            print line
            raise

    def iter_files(self):
        file_generator = self.all_files()


        for f in file_generator:
            if ".DS_Store" not in f:
                try:
                    gfile = gzip.open('./'+f)
                    for line in gfile:
                        self.process_line(line.strip())
                    gfile.close()
                except:
                    pass

class GeoCSVGnipDataProcessor(GnipDataProcessor):
    def process_chunk(self):
        # this is where we process the loaded json and write to a csv
        for item in self.chunk:
            if 'actor' in item:
                if 'preferredUsername' in item['actor']:
                    if item['actor']['preferredUsername'] in top_users:
                        top_users[item['actor']['preferredUsername']] += 1
                    else:
                        top_users[item['actor']['preferredUsername']] = 1
                

if __name__ == '__main__':
    
    csv = GeoCSVGnipDataProcessor('./spanish_march/', chunk_size=50)
    csv.iter_files()
    print top_users
