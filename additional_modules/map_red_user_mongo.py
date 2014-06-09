import simplekml
import json
import datetime
import csv
from bson.code import Code
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt
from random import choice
import operator
import csv


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

def closest_negighbor(geo_dict):
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



def map_points(input_file):

    geo_stuff = tw_spanish.map_reduce(mapper,reducer, "results")

    #print geo_stuff
    count = 0

    geo_dict = {}

    with open('./testmap_spanish_user.csv', 'wb') as csvfile:
        mapwriter = csv.writer(csvfile)

        mapwriter.writerow(['user','latitude','longitude','n_loc'])


        for doc in geo_stuff.find():
            geo_dict['user'] = doc['_id']['user']

            value = doc['value']

            if 'Geo' in value:
                geo_dict['location'] = [value['Geo']]
                geo_dict['n_loc'] = 1
            elif 'Geo_list' in value:
                geo_dict['location'] = value['Geo_list']
                geo_dict['n_loc'] = value['n_pts']

            geo_data = closest_negighbor(geo_dict)

            if geo_data != {}:
                mapwriter.writerow([geo_data['user'],geo_data['loc'][0],geo_data['loc'][1], geo_data['n_loc']])

            print geo_data
            count += 1

        print count

    # f = open(input_file, "r" )

    # with open('./testmap_spanish.csv', 'wb') as csvfile:

    #     mapwriter = csv.writer(csvfile)

    #     mapwriter.writerow(['time','latitude','longitude'])

        
    #     for each in f:
    #         tmp = each.split('\t')
    #         time = datetime.datetime.strptime(tmp[0][0:-5], '%Y-%m-%dT%H:%M:%S')
    #         geo = tmp[1].strip().split(', ')

    #         #print time, geo
            
    #         row = []

    #         row.append(time)
    #         row.append(geo[0])
    #         row.append(geo[1])
            
    #         try:
    #             mapwriter.writerow(row)
    #         except:
    #             mapwriter.writerow([unicode(s).encode("utf-8") for s in row])

            
if __name__ == '__main__':
    client = MongoClient()
    db = client.twitter_test
    tw_spanish = db.spanish_tweets       

    mapper = Code("""function () {
            emit({user:this.actor.id},{Geo: this.geo.coordinates});
    }
    """
    )



    reducer = Code("""function(key,values) {
        var list = [];
        var count = 0;
        values.forEach(function(value) {
            if(value.Geo){
                list.push(value.Geo);
                count+=1;
            }
        })
        return {Geo_list:list, n_pts:count};
        }
        """
        )
    
    ifile = './output.txt'
    map_points(ifile)

