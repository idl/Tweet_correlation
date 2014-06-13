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

# Global
idx = index.Index()
polygons = []
polygon_features = []


def extract_features(shape_f):
  global idx
  global polygons, polygon_features
  polygons_sf = shapefile.Reader(shape_f)
  polygon_shapes = polygons_sf.shapes()
  polygon_points = [q.points for q in polygon_shapes ]
  

  polygon_fields = {}
  for ind, each in enumerate(polygons_sf.fields):
    polygon_fields[each[0]] = ind -1 

  print polygon_fields

  polygon_features = [(f[polygon_fields['geoid10']], f[polygon_fields['namelsad10']]) for f in polygons_sf.records()]
  polygons = [Polygon(q) for q in polygon_points]

  count = -1
  for q in polygon_shapes:
    count +=1
    idx.insert(count, q.bbox)
  print "Shapefile Features Extracted"

def check_contains1(dp_data):
  # mongo_id = dp_data['result']['id']
  lat = dp_data['result']['latitude']
  lon = dp_data['result']['longitude']
  # shape_f = dp_data['shp_file']
  # poly_name = dp_data['poly_name']

  point_coord = [float(lon),float(lat)]
  point_geom = Point(float(lon),float(lat))
  
  for j in idx.intersection(point_coord):
    if point_geom.within(polygons[j]):
      dp_data['polygon_features'] = polygon_features[j]
      # update mongo record
      # dp_data['mongodb_conn'].update({"id": dp_data['result']['id']}, {"$set": {"polygon_features": dp_data['polygon_features']}})
      return dp_data
  return None



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


    def get_data(self):
        try:
            search_query = { '$or': [
              {"geo.type":"Point"},
              {"place.bounding_box.type":"Polygon"}
              ]
            }
            for doc in self.collection.find(search_query):
                if ('geo' in doc and doc['geo'] != None) and ('type' in doc['geo']) and (doc['geo']['type'] == "Point"):
                  result = {
                      'id' : doc['id'],
                      'latitude' : doc['geo']['coordinates'][0],
                      'longitude' : doc['geo']['coordinates'][1],
                      'type' : 'Point'
                  }
                  yield result
                
                elif ('place' in doc and doc['place'] != None) and 'bounding_box' in doc['place'] and 'type' in doc['place']['bounding_box'] and doc['place']['bounding_box']['type'] == "Polygon":
                  poly_coordinates = doc['place']['bounding_box']['coordinates']
                  centroid = get_poly_centroid(poly_coordinates)
                  result = {
                      'id' : doc['_id'],
                      'latitude' : centroid.x,
                      'longitude' : centroid.y,
                      'type' : 'Polygon'
                  }
                  yield result
        except:
            pass

def main():
  total = len(sys.argv)

  if total < 3:
    print "Utilization: python county_corelation.py <mongo_host> <mongo_db> <mongo_collection> <collection_query_field> <shape_file> <shape_polygon_field> "
    exit(0)

  mongo_db = {
        'host' : str(sys.argv[1]),
        'default_port' : 27017,
        'db' : str(sys.argv[2]),
        'collection' : str(sys.argv[3]),
        'query_field' : str(sys.argv[4]),
    }

  conn = mongo_host(mongo_db)

  extract_features(str(sys.argv[5]))

  pool = Pool(processes=cpu_count())

  count = 1

  idata = []
  for result in conn.get_data():
    dp_data = {}
    dp_data['mongodb_conn'] = conn
    dp_data['result'] = result
    dp_data['shp_file'] = str(sys.argv[5])
    dp_data['poly_name'] = str(sys.argv[6])
    idata.append(dp_data)
    count += 1
    if count % 500 == 0:
  
        responses = pool.imap_unordered(check_contains1, idata)
        num_tasks = len(idata)
        start_time = time.time()
        while (True):
          completed = responses._index
          if (completed == num_tasks): break
          percent = (float(completed)/float(num_tasks))*100
          print "%.3f" % percent," % complete. ", "Waiting for", num_tasks-completed, "tasks to complete..."
          time.sleep(2)
        idata = []
        end_time = time.time()
        print "total time taken this loop: ", end_time - start_time
        print "Found and wrote: %d" % count
  pool.close()



if __name__ == "__main__":
  main()
