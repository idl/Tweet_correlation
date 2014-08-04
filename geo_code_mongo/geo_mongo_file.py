from multiprocessing import Process, Queue, Pool, cpu_count
import json
import csv
import ast
import time
import sys
from operator import itemgetter
from shapely.wkb import loads
from bson.code import Code
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt
from random import choice
import numpy
import shapefile
from rtree import index
from shapely.geometry import Polygon, Point

def get_poly_centroid(coordinates):
    poly_list = []
    
    for each in coordinates[0]:
        poly_list.append((each[1], each[0]))

    if poly_list == sorted(poly_list):
      centroid = Point(poly_list[0][1], poly_list[0][0])
      return centroid
    else:
      polygon = Polygon(poly_list)
      try:
        centroid = polygon.centroid
        return centroid
      except Exception as e:
        print e

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.query_field = '$' + mongo_db['query_field']

    def total_docs(self):
        return self.collection.count()

    def get_data(self):
        try:
            search_query = { '$or': [
              {"geo.type":"Point"},
              {"place.bounding_box.type":"Polygon"}
              ]
            }
            for doc in self.collection.find(search_query):
                result = {}
                try:
                  if ('geo' in doc and doc['geo'] != None) and ('type' in doc['geo']) and (doc['geo']['type'] == "Point"):
                    result = {
                        'mongo_id' : doc['id'],
                        'latitude' : doc['geo']['coordinates'][0],
                        'longitude' : doc['geo']['coordinates'][1],
                        'type' : 'Point',
                        'language': doc['lang'],
                        'user' : {
                          'id': doc['user']['id'],
                          'screen_name' : doc['user']['screen_name'],
                          'location' : doc['user']['location'],
                          'lang' : doc['user']['lang'],
                        }
                    }
                  elif ('place' in doc and doc['place'] != None) and 'bounding_box' in doc['place'] and 'type' in doc['place']['bounding_box'] and doc['place']['bounding_box']['type'] == "Polygon":
                    poly_coordinates = doc['place']['bounding_box']['coordinates']
                    centroid = get_poly_centroid(poly_coordinates)
                    result = {
                        'mongo_id' : doc['id'],
                        'latitude' : centroid.x,
                        'longitude' : centroid.y,
                        'type' : 'Polygon',
                        'language': doc['lang'],
                        'user' : {
                          'id': doc['user']['id'],
                          'screen_name' : doc['user']['screen_name'],
                          'location' : doc['user']['location'],
                          'lang' : doc['user']['lang'],
                        }
                    }
                except Exception as e:
                  print "issue", e
                yield result
        except:
            pass

def main():
  total = len(sys.argv)

  if total < 5:
    print "Utilization: python county_corelation_file.py <mongo_host> <mongo_db> <mongo_collection> <collection_query_field> <output_file>"
    exit(0)

  mongo_db = {
        'host' : str(sys.argv[1]),
        'default_port' : 27017,
        'db' : str(sys.argv[2]),
        'collection' : str(sys.argv[3]),
        'query_field' : str(sys.argv[4]),
    }

  conn = mongo_host(mongo_db)

  total_docs = conn.total_docs()

  count = 1
  count_docs = 1
  try:
    with open(sys.argv[5], 'a') as jfile:
        # for doc in conn.collection.find():
        #   print doc

        for result in conn.get_data():
            # print result
            count_docs += 1
            if result != {}:
              json.dump(result,jfile)
              jfile.write('\n')
              count += 1
              if count % 50000 == 0:
                  print "Found and wrote: %d out of %d total docs" % (count, count_docs)
  except Exception as e:
    print "issue", e
  

  

if __name__ == "__main__":
  main()


