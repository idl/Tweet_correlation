from multiprocessing import Process, Queue, Pool, cpu_count
from osgeo import ogr
import json
import csv
import ast
import time
import sys
from operator import itemgetter
from shapely.wkb import loads


def check_contains(shape_f,lon1,lat1,poly_name):
  driver = ogr.GetDriverByName('ESRI Shapefile')
  try:
    polyshp = driver.Open(shape_f, 0)
    polylr = polyshp.GetLayer()
  except Exception as e:
    print e.msg()

  point_geom = ogr.Geometry(ogr.wkbPoint)
  point_geom.SetPoint_2D(0, float(lon1),float(lat1))

  pt_poly = []
  for feature in polylr:
    polyfeat = polylr.GetNextFeature()
    poly_geom = polyfeat.GetGeometryRef()
    if poly_geom.Contains(point_geom):
      poly_name = polyfeat.GetField(poly_name)
      pt_poly = [[lon1,lat1],poly_name]
      return pt_poly
  return pt_poly

def process_distance(dp_data):
  coordinates = dp_data['coordinates']
  try:
    coordinates = dp_data['coordinates']

    pt_poly = check_contains(dp_data['shp_file'], coordinates[1],coordinates[0],dp_data['poly_name'])

    if pt_poly != []:
      dp_data['poly_name'] = pt_poly[1]
    return dp_data
  except:
    pass

def read_input(shp_file,poly_name,i_file):
  idata = []
  with open(i_file, 'rU') as f:
    reader = csv.reader(f)
    next(reader, None)
    for row in reader:
      dp_data = {
        'dp_id': row[0],
        'coordinates':ast.literal_eval(row[1]),
        'tweet':row[2],
        'created_at':row[3],
        'user':row[4],
        'shp_file':shp_file,
        'poly_name':poly_name
      }
      idata.append(dp_data)
  f.close()
  return idata

def write_to_csv(ofile,odata):
  with open(ofile, 'wb') as csvfile:
    mapwriter = csv.writer(csvfile)

    mapwriter.writerow(['id','longitude','latitude', 'poly_name', 'tweet','created_at','user'])

    for each in odata:
      mapwriter.writerow([each['dp_id'],each['coordinates'][1], each['coordinates'][0], each['poly_name'], each['tweet'],each['created_at'],each['user']])


def main():
  total = len(sys.argv)

  if total < 3:
    print "Utilization: python apply_distance.py <shape_file> <shape_polygon_field> <input_csv_file> <output_csv_file>"
    exit(0)

  pool = Pool(processes=cpu_count())


  idata = read_input(str(sys.argv[1]),str(sys.argv[2]),str(sys.argv[3]))

  num_tasks = len(idata)  

  #imap
  responses = pool.imap_unordered(process_distance, idata)

  while (True):
    completed = responses._index
    if (completed == num_tasks): break
    percent = (float(completed)/float(num_tasks))*100
    print "%.3f" % percent," % complete. ", "Waiting for", num_tasks-completed, "tasks to complete..."
    time.sleep(2)


  pool.close()

  responses = [x for x in responses if x is not None]

  idata = write_to_csv(str(sys.argv[4]),responses)


if __name__ == "__main__":
  main()
