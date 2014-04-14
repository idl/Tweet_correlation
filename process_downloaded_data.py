import os,sys
import gzip, json
import csv

class GnipDataProcessor(object):

    def __init__(self, i_path, o_file, chunk_size=50):
        self.path = i_path
        self.output_file = csv.writer(open(os.path.join(self.path, o_file), 'w+'))
        self.chunk = []
        self.chunk_size = chunk_size
        self.output_file.writerow(['id','geo','tweet','created_at','user'])

    def all_files(self):
        for path, dirs, files in os.walk(self.path):
            for f in files:
                yield os.path.join(path, f)

    def iter_files(self):
        file_generator = self.all_files()

        for f in file_generator:
            try:
                gfile = gzip.open('./'+f)
                for line in gfile:
                    self.process_line(line)
                gfile.close()
            except:
                pass

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

    def process_chunk(self):
        # this is where we process the loaded json and write to a csv
        for item in self.chunk:
            if 'geo' in item:
                tw_id = item['id']
                geo = ", ".join([str(i) for i in item['geo']['coordinates']])
                tweet = item['body']
                created_at = item['postedTime']
                user = item['actor']['preferredUsername']
                try:
                    row = [tw_id,geo,tweet,created_at,user]
                    self.output_file.writerow([unicode(s).encode("utf-8") for s in row])
                except:
                    print "issue writing"

if __name__ == '__main__':
    total = len(sys.argv)

    if total < 2:
        print "Utilization: python process_downloaded_data.py <input_dir> <output_file>"
        exit(0)

    csv = GnipDataProcessor(str(sys.argv[1]),str(sys.argv[2]), chunk_size=50)
    csv.iter_files()
