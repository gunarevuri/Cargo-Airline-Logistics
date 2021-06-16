import json
import datetime
import math
import time
from pprint import pprint
import random
import json
from flask.json import jsonify
import requests

import pymongo
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo import ReturnDocument
from bson.objectid import ObjectId

import ssl

city_locations = {}
url = 'http://localhost:5000'


def calcute_routes_distance(routes,arrive_at,send_to):
    """
    Returns: distance a plane going to travel (sum of all city distances in route field)
    """
    global city_location
    distance = 0
    if len(routes)>1:
        for i in range(len(routes)-1):
            if routes[i] != arrive_at:
                lat1,lon1 = math.radians(city_locations[routes[i]][1]), math.radians(city_locations[routes[i-1]][0])
                lat2, lon2 = math.radians(city_locations[routes[i+1]][1]), math.radians(city_locations[routes[i]][0])
                R = 6373.0
                change_lon = lon2 - lon1
                change_lat = lat2 - lat1
                
                hav = math.sin(change_lat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(change_lon / 2)**2
                rec = 2 * math.atan2(math.sqrt(hav), math.sqrt(1 - hav))
                distance += R * rec
            else:
                break
    return distance

# Function to add parcel to nearest plane, by calculating distancce between parcel and each plane (long,lat) coordinates. 
def add_route(inserted_id,arrive_at,send_to):
    print(arrive_at)
    print(send_to)



    URL = 'mongodb+srv://admin:admin123@cluster0.llwjl.mongodb.net/myFirstDatabase?retryWrites=true&w=1'
    client = pymongo.MongoReplicaSetClient(URL,ssl=True,ssl_cert_reqs=ssl.CERT_NONE)
    db = client.get_database('logistics')
    planes_col = db.get_collection(name='planes',read_concern=ReadConcern(level='majority'))
    cargos_col = db.get_collection(name='cargos',write_concern=WriteConcern(w='majority'))
    cities_col = db.get_collection(name='cities')
    
    global city_locations
    addfields = {"$addFields":{"name":"$_id","location":"$position"}}
    project = {"$project":{"_id":0,"position":0}}
    cities = list(cities_col.aggregate([addfields,project]))
    for city in cities:
        city_locations[city['name']] = city['location']
        
    addfields = {"$addFields":{"callsign":"$_id"}}
    project = {"$project":{"_id":0,'landed':{'$ifNull':['$landed','']},'callsign':1,'currentLocation':1,'heading':1,'route':1}}
    planes = list(planes_col.aggregate([addfields,project]))
    
    plane_distance = {}
    
    lat1 = math.radians(city_locations[arrive_at][1])
    lon1 = math.radians(city_locations[arrive_at][0])
    R = 6373.0
    # Iterate over each plane adds routes distance + plane to parcel distance sort them and assign parcel to nearest plane.
    for plane in planes:
        lat2 = math.radians(plane['currentLocation'][1])
        lon2 = math.radians(plane['currentLocation'][0])
        change_lon = lon2 - lon1
        change_lat = lat2 - lat1
        routes_distance = calcute_routes_distance(plane['route'],arrive_at=arrive_at,send_to=send_to)
        
        hav = math.sin(change_lat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(change_lon / 2)**2
        rec = 2 * math.atan2(math.sqrt(hav), math.sqrt(1 - hav))
        distance = R * rec + routes_distance
        plane_distance[plane['callsign']] = {"route":plane['route'],"distance":distance}
        
    sorted_plane_distances = dict(sorted(plane_distance.items(), key=lambda item: item[1]['distance'] ))
    plane_id=  next(iter(sorted_plane_distances))
    print(inserted_id)
    updated_cargo = cargos_col.find_one_and_update({"_id":ObjectId(inserted_id)},{"$set":{"courier":plane_id}},return_document=ReturnDocument.AFTER)
    print("assigned {}".format(plane_id))
    print(updated_cargo)
    planes_col.find_one_and_update({"_id":plane_id},{"$push":{"route":{"$each":[arrive_at,send_to]}}})
    return None
    # except Exception as e:
    #     print(str(e))
    #     return jsonify({'Error':str(e)}),400