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

  polygon_features = [(f[polygon_fields['GEOID10']], f[polygon_fields['NAMELSAD10']]) for f in polygons_sf.records()]
  polygons = [Polygon(q) for q in polygon_points]

  count = -1
  for q in polygon_shapes:
    count +=1
    idx.insert(count, q.bbox)
  print "Shapefile Features Extracted"

def check_contains1(dp_data):
  lat = dp_data['result']['latitude']
  lon = dp_data['result']['longitude']

  point_coord = [float(lon),float(lat)]
  point_geom = Point(float(lon),float(lat))
  

  for j in idx.intersection(point_coord):
    if point_geom.within(polygons[j]):
      dp_data['result']['county_geoid'] = polygon_features[j][0]
      # dp_data['result']['county_name'] = polygon_features[j][1]
      return dp_data['result']
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


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def main():
  total = len(sys.argv)

  if total < 2:
    print "Utilization: python county_corelation.py <shape_file> <shape_polygon_field> <input_file> "
    exit(0)

  #insert into mongodb local
  inst_client = MongoClient()
  inst_db = inst_client['twitter3']
  inst_coll = inst_db['twitter_geo']

  extract_features(str(sys.argv[1]))

  pool = Pool(processes=cpu_count())

  count = 1

  idata = []

  with open(str(sys.argv[3]),'rb') as f:
    for line in f:
      dp_data = {}
      dp_data['result'] = json.loads(line)
      dp_data['shp_file'] = str(sys.argv[1])
      dp_data['poly_name'] = str(sys.argv[2])
      
      idata.append(dp_data)
      
      count += 1
      
      if count % 30000 == 0:
        num_tasks = len(idata)
        start_time = time.time()

        pool = Pool(processes=cpu_count())

        responses = pool.imap_unordered(check_contains1, idata)

        pool.close()
        pool.join()
        
        responses = [x for x in responses if x is not None]

        #insert bulk into mongo
        for each in list(chunks(responses, 10000)):
          try:
            inst_coll.insert(each)
          except:
            try:
              for each_one in each:
                inst_coll.insert(each_one)
            except:
              pass

        idata = []
        end_time = time.time()
        print "\nTotal time taken this loop: ", end_time - start_time
        print "Found and wrote: %d, %d\n" % (count, len(responses))
  # pool.close()
  f.close()
  print "Done!!!"




if __name__ == "__main__":
  main()


