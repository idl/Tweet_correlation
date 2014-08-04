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
import operator
import re
from BeautifulSoup import BeautifulSoup

def haversine(lon1, lat1, lon2, lat2):
    """
Calculate the great circle distance between two points
on the earth (specified in decimal degrees)
"""
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def closest_neighbor(geo_dict):
    data = {}

    if geo_dict['n_loc'] == 1:
        data['user'] = geo_dict['user']
        data['loc'] = geo_dict['location'][0]
    elif geo_dict['n_loc'] == 2:
        data['user'] = geo_dict['user']
        data['loc'] = choice(geo_dict['location'])
    elif geo_dict['n_loc'] > 2:
        pt_dict = {}
        for i in range(len(geo_dict['location'])):
            distance_list = []
            for j in range(len(geo_dict['location'])):
                distance = haversine(geo_dict['location'][i][0],geo_dict['location'][i][1],geo_dict['location'][j][0],geo_dict['location'][j][1])
                distance_list.append(distance)
            pt_dict[i] = reduce(lambda x, y: x + y, distance_list) / len(distance_list)
        
        data['user'] = geo_dict['user']
        data['loc'] = geo_dict['location'][max(pt_dict.iteritems(), key=operator.itemgetter(1))[0]]
    else:
        return data

    data['n_loc'] = geo_dict['n_loc']
    return data

def extract_features(shape_f):
    global polygons, polygon_features
    polygons_sf = shapefile.Reader(shape_f)
  

    polygon_fields = {}
    for ind, each in enumerate(polygons_sf.fields):
        polygon_fields[each[0]] = ind - 1

    polygon_features = {}
    for f in polygons_sf.records():
        polygon_features[f[polygon_fields['GEOID10']]] = [f[polygon_fields['NAMELSAD10']], f[polygon_fields['DP0010001']]]

    # polygon_features = [({f[polygon_fields['GEOID10']] : [f[polygon_fields['NAMELSAD10']], f[polygon_fields['DP0010001']]]}) for f in polygons_sf.records()]
    print "Shapefile Features Extracted"
    return polygon_features

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.query_field = '$' + mongo_db['query_field']

    def get_county_total(self):
    	pipeline = [{'$group':{'_id': {'geo_code':"$county_geoid"}, 'total':{'$sum':1}}}]
        d = self.collection.aggregate(pipeline=pipeline)
        return d['result']

    def get_lang_total(self):
        pipeline = [{'$group':{'_id': {'lang':"$language"}, 'total':{'$sum':1}}}]
        d = self.collection.aggregate(pipeline=pipeline)
        return d['result']

    def get_source_info(self):
        for doc in self.collection.find():
            if doc['source'] != "":
                yield doc['source']
            else:
                yield {}

    def get_usr_geo(self):
        pipeline = [{'$group':{
                            '_id': {'user_id':"$user.id"}, 
                            'location':{'$push': {
                                    'long':"$longitude",
                                    'lat':"$latitude",
                                    'county_geoid':"$county_geoid",
                                    }
                                }
                            }
                        }]

        cursor = self.collection.aggregate(pipeline=pipeline,allowDiskUse=True, cursor={})

        results = {}
        r_list = []
        count = 0
        for each in cursor:
            if each != None:
                loc_list = []
                for each_one in each['location']:
                    loc = [each_one['long'], each_one['lat'], each_one['county_geoid']]
                    loc_list.append(loc)

                
                data = {
                    'user': each['_id']['user_id'],
                    'location' : loc_list,
                    'n_loc' : len(loc_list),
                }
        
                count += 1    
                if count % 5000 == 0:
                    print "Done with %d " % count

                r_list.append(data)

        print "Done getting Data from Mongo: %d" % count

        count = 0
        total_docs = len(r_list)
        idata = []
        for data in r_list:
            idata.append(data)
            count += 1
            if count % 30000 == 0 or count == total_docs:
                start_time = time.time()
                pool = Pool(processes=cpu_count())

                responses = pool.imap_unordered(closest_neighbor, idata)

                pool.close()
                pool.join()
        
                for pt in responses:
                    if pt != None:
                        if pt['loc'][2] in results:
                            results[pt['loc'][2]] += 1
                        else:
                            results[pt['loc'][2]] = 1                        

                idata = []
                end_time = time.time()
                print "\nTotal time taken this loop: ", end_time - start_time
                print "Calculated: %d\n" % (count)

        print "Done with Calculations: %d" % count
            
        f_list = []
        
        for key, value in results.iteritems():
            f_result = {
                '_id' : {
                    'geo_code' : key
                },
                'total': value
            }
            f_list.append(f_result)

        return f_list
        # return d['result']

def main():
    total = len(sys.argv)

    if total < 3:
        print "Utilization: python county_corelation.py <mongo_host> <mongo_db> <mongo_collection> <collection_query_field> <shape_file> <shape_polygon_field> <query_type>"
        exit(0)

    mongo_db = {
    		'host' : str(sys.argv[1]),
    		'default_port' : 27017,
		'db' : str(sys.argv[2]),
		'collection' : str(sys.argv[3]),
		'query_field' : str(sys.argv[4]),
		}

    conn = mongo_host(mongo_db)

    query_type = str(sys.argv[7])
    f_name = query_type + '.csv'

    if query_type == 'tweets_county' or query_type == 'users_county':
        if query_type == 'tweets_county':
            result = conn.get_county_total()       
        if query_type == 'users_county':
            result = conn.get_usr_geo()

        polygon_features = extract_features(str(sys.argv[5]))
        
        with open(f_name, 'wb') as csvfile:
            file_out = csv.writer(csvfile)
            file_out.writerow(['County','Geo_id', 'Census_pop', 'Tweets_data'])

            d = {}
            for each in result:
                geo_code = each['_id']['geo_code']
                if geo_code in polygon_features:
                    d_list = [polygon_features[geo_code][0], geo_code, polygon_features[geo_code][1], each['total']]
                    file_out.writerow(d_list)
                    d[geo_code] = d_list

    if query_type == 'lang_total':
        result = conn.get_lang_total()
        from lang_codes import lang_code

        with open(f_name, 'wb') as csvfile:
            file_out = csv.writer(csvfile)
            file_out.writerow(['Language','Number of Tweets'])
            for each in result:
                print each
                if each['_id']['lang'] in lang_code:
                    d_list = [lang_code[each['_id']['lang']],each['total']]
                else:
                    d_list = [each['_id']['lang'],each['total']]
                file_out.writerow(d_list)

    if query_type == 'source':
        result = conn.get_source_info()
        total = {}
        count = 0
        for each in result:
            if each != {}:
                each = BeautifulSoup(each)
                if each.text not in total:
                    total[each.text] = 1
                else:
                    total[each.text] += 1

            count += 1

            if count % 50000 == 0:
                print "Done with %d" % count

        with open(f_name, 'wb') as csvfile:
            file_out = csv.writer(csvfile)
            file_out.writerow(['Source','Number of Tweets'])
            for key, value in total.iteritems():
                try:
                    d_list = [key, value]
                    file_out.writerow(d_list)
                except:
                    pass
        






if __name__ == "__main__":    
    main()
