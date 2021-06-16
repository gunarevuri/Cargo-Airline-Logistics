#!/usr/bin/python3


from os import write
import pymongo
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo import ReturnDocument
from pymongo.errors import AutoReconnect,PyMongoError

from bson.objectid import ObjectId
from bson import json_util

import datetime
from flask import Flask, request, send_from_directory, jsonify, abort,render_template
from flask.json import JSONEncoder

import routing
import json
import requests
import ssl

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        return json_util.default(obj)
    
app = Flask(__name__, static_url_path="")


"""

Plane

 {
    "callsign": "CARGO1234",
    "currentLocation": [ 55.2, -18.3],
    "heading": 165,
    "route": ["London","Paris"],
    "landed": "Madrid",
}

City
{
    "name": "London",
    "country": "United Kingdom",
    "location": [ 52.0, -0.1 ],
}

Cargo
{
    "id": "abcdabcdabcd",
    "destination": "Paris",
    "location": "Berlin",
    "courier": "CARGO1234",
    "received": "2020-06-15T11:34:05Z,
    "status": "in process" OR "delivered",
    "delivered_time":time,
    "Origin":"Harare"
    
    "history":[
    ]
}

Make sure you create required indexes in Cargo collection. 

    db.cargoes.createIndex({'location':1,'status':1})
 
   
PlaneHistory 
{
    "_id": "60a012288e34fda6cc92c13e",
    "plane": "CARGO17",
    "location": "Madrid",
    "status": "in process",
    "cargo_id": "609ffbdf87073877450b6a1d",
    "operation": "drop",
    "date": "2021-05-15T23:55:44.862Z"
}
Make sure you create required indexes for PlaneHistory collection
    db.planeHistory.createIndex({"plane":1,"date":1,"operation":1})
    
`deltime` collection contain following structure. 

{ 

	"_id" : 7, 

	"delivery_time" : ISODate("2021-05-18T15:14:08.101Z"), 

	"delivery_sum" : 0.283479212, 

	"cargo_id" : "60a38ae6e704c461fa86846f" 

} 

“ID” itself delivered parcels count. 

"""


# Anything that isn't part of the API treat as a static file
# To be clear we aren't rendering anything server side - this is a rich
# Client app and web services

#Return the App with no URL - anything that isn't a route is returned as a file
#from the 'Static' directory by default

url = 'http://localhost:5000'
headers = {'content-type': 'application/json'}
URL = 'mongodb+srv://admin:admin123@cluster0.llwjl.mongodb.net/myFirstDatabase?retryWrites=true&w=1'
client = pymongo.MongoReplicaSetClient(URL,ssl=True,ssl_cert_reqs=ssl.CERT_NONE)

@app.route("/")
def root():
    return app.send_static_file("index.html")

#Get All City Info
@app.route("/cities", methods=["GET"])
def get_all_cities():
    global URL
    try:

        db = client.get_database(name='logistics')
        cities_col = db.get_collection(name='cities')
        addfields = {"$addFields":{"name":"$_id","location":"$position"}}
        project = {"$project":{"_id":0,"position":0}}
        
        cities = list(cities_col.aggregate([addfields,project]))
        return jsonify(cities),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    

        

#Get infor for a specific City

@app.route("/cities/<id>", methods=["GET"])
def get_city_info(id):
    global URL
    id = str(id)
    try:

        db = client.get_database(name='logistics')
        cities_col = db.get_collection(name='cities')
        match = {"$match":{"_id":id}}
        addfields = {"$addFields":{"name":"$_id","location":"$position"}}
        project = {"$project":{"_id":0,"position":0}}
        
        # Agg pipeline.
        city = list(cities_col.aggregate([match,addfields,project]))
        
        if len(city) == 0:
            return jsonify({"city":city}),404
        
        return jsonify(city[-1]),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),404
    


#Get all Plane info

@app.route("/planes", methods=["GET"])
def get_all_planes():
    global URL
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes',read_concern=ReadConcern(level='majority'))
        addfields = {"$addFields":{"callsign":"$_id"}}
        project = {"$project":{"_id":0,'landed':{'$ifNull':['$landed','']},'callsign':1,'currentLocation':1,'heading':1,'route':1}}
        planes = list(planes_col.aggregate([addfields,project]))

        return jsonify(planes),200
        
    
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    


        
#Get a Specific Plane

@app.route("/planes/<id>", methods=["GET"])
def get_plane_info(id):
    global URL
    id = str(id)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes')
        
        match = {"$match":{"_id":id}}
        addfields = {"$addFields":{"callsign":"$_id"}}
        project = {"$project":{"_id":0,'landed':{'$ifNull':['$landed','']},'callsign':1,'currentLocation':1,'heading':1,'route':1}}
        
        planes = list(planes_col.aggregate([match, addfields, project]))
        if len(planes) == 0:
            return jsonify(planes),404
        

        return jsonify(planes[-1]),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    



#Update Location and Heading for a Plane 

@app.route("/planes/<id>/location/<location>/<heading>", methods=["PUT"])
def update_plane_location(id, location, heading):
    global URL
    id ,currentLocation, heading = str(id), list(map(float,str(location).strip().split(','))), float(heading)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes')
        
        query = {"_id":id}
        update_ = {"$set":{"currentLocation":currentLocation,"heading":heading}}
        # CARGO0, 
        plane = planes_col.find_one_and_update(query,update=update_,return_document=ReturnDocument.AFTER)
        
        if plane is None:
            return jsonify({"planes":plane}),400

        plane['callsign'] = plane['_id']
        plane.pop('_id')
        plane['heading'] = float(plane['heading'])
        return jsonify(plane),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    

    
    
#Update Location and Heading for a Plane  incluing an airport

@app.route("/planes/<id>/location/<location>/<heading>/<city>", methods=["PUT"])
def update_plane_location_city(id, location, heading, city):
    global URL
    id ,currentLocation, heading, city = str(id), list(map(float,str(location).strip().split(','))), float(heading), str(city)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes',write_concern=WriteConcern(w='majority'))
        
        cities_col = db.get_collection(name='cities')
        # CARGO0, 
        city_bool = cities_col.find_one({"_id":city})
        if city_bool is None:
            return jsonify({"Error":"City not found"}),400
        
        plane = planes_col.find_one_and_update({"_id":id},{"$set":{"currentLocation":currentLocation,"heading":heading,"landed":city},'$pull':{'route':city}},return_document=ReturnDocument.AFTER)
        
        
        if plane is None:
            return jsonify(plane),404
        plane['callsign'] = plane['_id']
        plane.pop('_id')
        return jsonify(plane),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    


# Remove the current routed destination (Pop from list)

@app.route("/planes/<id>/route/destination", methods=["DELETE"])
def remove_plane_destination(id):
    global URL
    id = str(id)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes')

        query = {"_id":id}
        update = {"$pop":{"route":-1}}
        plane = planes_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        
        if plane is None:
            return jsonify(plane),404
        
        plane['callsign'] = plane['_id']
        plane.pop('_id')
        return jsonify(plane),200
    
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    




# Replace the whole Route with a new Single destination
@app.route("/planes/<id>/route/<city>", methods=["PUT"])
def replace_plane_route(id, city):
    global URL
    id = str(id)
    city = str(city)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes')
        cities_col = db.get_collection(name='cities')
        # CARGO0, 
        city_bool = cities_col.find_one({"_id":city})
        if city_bool is None:
            return jsonify({"Error":"City not found"}),400
        
        query = {"_id":id}
        update = {"$set":{"route":[city]}}
        
        # Need to update projection with name by default withought any outside operations.
        
        plane = planes_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        
        if plane is None:
            return jsonify({"planes":plane}),404
        
        plane['callsign'] = plane['_id']
        plane.pop('_id')
        return jsonify(plane),200
    
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400
    




# Add a a the  destination to the Route
@app.route("/planes/<id>/route/<city>", methods=["POST"])
def add_plane_destination(id, city):
    global URL
    id = str(id)
    city = str(city)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planes')
        cities_col = db.get_collection(name='cities')
        # CARGO0, 
        city_bool = cities_col.find_one({"_id":city})
        if city_bool is None:
            return jsonify({"Error":"City not found"}),400
        
        query = {"_id":id}
        update = {"$push":{"route":city}}
        projection = {'name':'$_id',"_id":0,'currentLocation':1,'heading':1,'route':1,'landing':{'$ifNull':['$landing','']}}
        
        # Need to update projection with name by default withought any outside operations.
        
        plane = planes_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        
        if plane is None:
            return jsonify({"planes":plane}),404
        
        plane['callsign'] = plane['_id']
        plane.pop('_id')
        return jsonify(plane),200
    
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400




# Add a completely new cargo at a location with a destination

@app.route("/cargo/<location>/to/<destination>", methods=["POST"])
def new_cargo(location, destination):
    global URL
    location = str(location)
    destination = str(destination)
    
    try:
        db = client.get_database(name='logistics')
        #write conern majority for parcels that were getting created.
        cargos_col = db.get_collection(name='cargos',write_concern=WriteConcern(w='majority'))
        cities_col = db.get_collection(name='cities')
        # CARGO0, 
        city_bool = list(cities_col.find({"_id":{"$in":[location,destination]}}))
        if city_bool is None and len(city_bool)<2:
            return jsonify({"Error":"City not found"}),400
        
        doc = {"destination":destination,
               "location":location,
               "courier":None,
               "status":"in process",
               "received":datetime.datetime.now(),
               "delivered_time":None,
               "origin":location,
               "history":[]
               }
        inserted_id = cargos_col.insert_one(doc).inserted_id
        return jsonify({"id":str(inserted_id)}),200
    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400

    
        
    

# Flag a specific cargo as having been delivered
@app.route("/cargo/<id>/delivered", methods=["PUT"])
def mark_delivered(id):
    global URL
    global url,headers
    id = ObjectId(id)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        
        delivered_time = datetime.datetime.now()
        
        query = {"_id":id}
        update = {"$set":{"delivered_time":delivered_time,"status":"Delivered"}}
        
        cargo = cargos_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.BEFORE)
        cargo['id'] = str(cargo['_id'])
        cargo.pop('_id')
        data = {"location":cargo['location'],"status":"delivered","courier":cargo['courier']}
        requests.post("%s/cargo/%s/reference" % (url,cargo['id']),data=json.dumps(data),headers=headers)
        data = {'plane':cargo['courier'],'location': cargo['location'], 'status':"delivered",'cargo_id':cargo['id'],
                'operation':'drop'}
        requests.post("%s/planes/reference/" % (url), data=json.dumps(data), headers=headers)
        
        data = {'cargo_id':str(id)}
        requests.post('%s/avg_delivery/'%(url),data = json.dumps(data),headers=headers)
        
        if cargo is None:
            return jsonify({"cargo":cargo}),400
        return jsonify(cargo),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400


# Assign Cargo Courier to a cargo - it will move to them on arrival

@app.route("/cargo/<id>/courier/<courier>", methods=["PUT"])
def assign_courier(id, courier):
    global URL
    id = ObjectId(id)
    courier = str(courier)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        
        query = {"_id":id}
        update = {"$set":{"courier":courier}}
        
        cargo = cargos_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        if cargo is None:
            return jsonify({"cargo":cargo}),400
        cargo['id'] = str(cargo['_id'])
        cargo.pop('_id')
        return jsonify(cargo),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400


# Remove Cargo Courier - Remove the specified courier from a cargo (usually on arrival and offloading)
@app.route("/cargo/<id>/courier", methods=["DELETE"])
def remove_courier(id):
    global URL
    global url,headers
    id = ObjectId(id)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        
        query = {"_id":id}
        update = {"$set":{"courier":None}}
        
        cargo = cargos_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        if cargo is None:
            return jsonify({"cargo":cargo}),400
        cargo['id'] = str(cargo['_id'])
        cargo.pop('_id')
        return jsonify(cargo),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400



# Move Cargo - Change the location of a cargo, this should check it's not teleporting
@app.route("/cargo/<id>/location/<location>", methods=["PUT"])
def update_location(id, location):
    global URL
    global url
    id = ObjectId(id)
    location = str(location)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        planes_col = db.get_collection(name='planes')
        planes_l = list(planes_col.find({},{'_id':1}))
        planes_list = []
        for plan in planes_l:
            planes_list.append(plan['_id'])
        query = {"_id":id}
        update = {"$set":{"location":location}}
        
        cargo = cargos_col.find_one_and_update(filter=query,update=update,return_document=ReturnDocument.AFTER)
        if cargo is None:
            return jsonify(cargo),400
        cargo['id'] = str(cargo['_id'])
        cargo.pop('_id')
        data = {"location":location,"status":"in process","courier":cargo['courier']}
        requests.post("%s/cargo/%s/reference" % (url,cargo['id']),data=json.dumps(data),headers={'content-type': 'application/json'})
        if location in planes_list:
            data = {'plane':cargo['courier'],'location': cargo['location'], 'status':"in process",'cargo_id':cargo['id'],
                            'operation':'pick up'}
        else:
            data = {'plane':cargo['courier'],'location': cargo['location'], 'status':"in process",'cargo_id':cargo['id'],
                'operation':'drop'}
        requests.post("%s/planes/reference/" % (url), data=json.dumps(data), headers=headers)

        return jsonify(cargo),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400



# Get all cargo at a given location (Plane or City)
@app.route("/cargo/location/<location>", methods=["GET"])
def get_cargo(location):
    global URL
    location = str(location)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        
        match = {"$match":{"location":location,"status":"in process"}}
        addFields = {"$addFields":{"id":{"$toString":"$_id"}}}
        project = {"$project":{"_id":0}}
        
        cargos = list(cargos_col.aggregate([match,addFields,project]))
        
        if cargos is None:
            return jsonify(cargos),400
        
        return jsonify(cargos),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400

    

@app.route('/cargo/<id>/reference', methods=['GET','POST'])
def cargo_history(id):
    global URL
    id = ObjectId(id)
    data = request.get_json()
    try:
        
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        data['timestamp'] = datetime.datetime.now()
        inserted_id = cargos_col.find_one_and_update({"_id":id},{"$push":{"history":data}})
        data['id'] = str(id)
        
        return jsonify(data),200
    except Exception as e:
        print(str(e))
        return jsonify({"Error":str(e)}),400

    
@app.route('/cargoes/avg_time',methods=['GET','POST'])
def avg_time():
    global URL
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        avg_time = list(cargos_col.aggregate([{"$match":{"status":"Delivered"}},{"$addFields":{"time":{"$subtract":["$delivered_time","$received"]}}},{"$group":{"_id":None,"avg_time":{"$avg":"$time"}}}]))[-1]['avg_time']/1000
        
        return jsonify({"avg_time":avg_time,"measured_in":"seconds"})
        
    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400

    
    
@app.route('/planes/reference/', methods=['POST','GET'])
def plane_reference():
    global URL
    # build index on plane,operation.
    data = request.get_json()
    data['cargo_id'] = str(data['cargo_id'])
    data['date'] = datetime.datetime.today()
    try:
        

        db = client.get_database(name='logistics')
        planes_hist = db.get_collection(name='planesHistory',write_concern=WriteConcern(w='majority'))
        inserted_id = planes_hist.insert_one(data).inserted_id
        
        return jsonify({"id":str(inserted_id)}),200
        
    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400

    
@app.route('/planes/profile')
def planes_profile():
    global URL
    try:

        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planesHistory',write_concern=WriteConcern(w='majority'))

        
        addFields = {"$addFields":{"time":
                {"$dateToString":
                    {"format":"%Y-%m-%d","date":"$date"
                    }
                }
            }}
        groupby = {"$group":{"_id":{"plane":"$plane","time":"$time","operation":"$operation"},
                    'count':{'$sum':1},
                    'output':{'$push':{'location':'$location','cargo':'$cargo_id','operation':'$operation','plane':'$plane','date':'$time'
                                        }
                                }
                    }
        }
        sort = {"$sort":{"_id.plane":1}}
        project = {'$project':{'plane':'$_id.plane','Date':'$_id.time','operation':'$_id.operation','_id':0,'count':1,'transactions':'$output'
                               }
                   }
        
        data = list(planes_col.aggregate([addFields, groupby, sort, project]))
        
        return jsonify(data),200
        
        
    except Exception as e:
        return jsonify({"Error":str(e)}),400
    

        
@app.route('/planes/<id>/profile')
def plane_profile(id):
    global URL
    
    """
    Make sure you have created index on planesHistory collection
    db.planesHistory.createIndex({"plane":1,'date':1})
    """
    
    id = str(id)
    try:
        client = pymongo.MongoReplicaSetClient(URL,ssl=True,ssl_cert_reqs=ssl.CERT_NONE)
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planesHistory',read_concern=ReadConcern(level='majority'),write_concern=WriteConcern(w='majority'))
        
        match = {'$match':{'plane': id}}
        
        addFields = {"$addFields":{"time":
                {"$dateToString":
                    {"format":"%Y-%m-%d","date":"$date"
                    }
                }
            }}
        groupby = {"$group":{"_id":{"time":"$time","operation":"$operation"},
                    'count':{'$sum':1}
                    }
                }
        sort = {"$sort":{"_id.time":1,'_id.operation':-1
                         }
                }
        project = {'$project':{'plane':id,'Date':'$_id.time','operation':'$_id.operation','_id':0,'count':1
                               }
                   }
        data = list(planes_col.aggregate([match, addFields, groupby, sort, project ]))
        return jsonify(data),200
        
        
    except Exception as e:
        return jsonify({"Error":str(e)}),400
    

        
@app.route('/planes/<id>/profile/drops')
def plane_profile_drops(id):
    global URL
    """
    Make sure you have created index on planesHistory collection
    db.planesHistory.createIndex({"plane":1,'date':1})
    """
    
    id = str(id)
    try:
        client = pymongo.MongoReplicaSetClient(URL,ssl=True,ssl_cert_reqs=ssl.CERT_NONE)
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planesHistory',read_concern=ReadConcern(level='majority'),write_concern=WriteConcern(w='majority'))
        
        match = {'$match':{'plane': id}}
        addFields = {"$addFields":{"time":{"$dateToString":{"format":"%Y-%m-%d","date":"$date"}}}}
        groupby = {"$group":{"_id":{"time":"$time","operation":"$operation"},'count':{'$sum':1}} }
        match_2 = {'$match':{"_id.operation":"drop"}}
        sort = {"$sort":{"_id.time":1 }}
        project = {'$project':{'plane':id,'Date':'$_id.time','operation':'$_id.operation','_id':0,'count':1}}
        data = list(planes_col.aggregate([match, addFields, groupby,match_2, sort, project ]))
        
        return jsonify(data),200
    except Exception as e:
        return jsonify({"Error":str(e)}),400
    

        
@app.route('/planes/<id>/profile/pickups', methods=['GET'])
def plane_profile_pickups(id):
    global URL
    
    """
    Make sure you have created index on planesHistory collection
    db.planesHistory.createIndex({"plane":1,'date':1})
    """
    
    id = str(id)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planesHistory')
        
        match = {'$match':{'plane': id}}
        addFields = {"$addFields":{"time":{"$dateToString":{"format":"%Y-%m-%d","date":"$date"}}}}
        groupby = {"$group":{"_id":{"time":"$time","operation":"$operation"},'count':{'$sum':1}} }
        match_2 = {'$match':{"_id.operation":"pick up"}}
        sort = {"$sort":{"_id.time":1 }}
        project = {'$project':{'plane':id,'Date':'$_id.time','operation':'$_id.operation','_id':0,'count':1}}
        data = list(planes_col.aggregate([match, addFields, groupby,match_2, sort, project ]))
        
        return jsonify(data),200
    except Exception as e:
        return jsonify({"Error":str(e)}),400
    
    finally:
        client.close()
        
@app.route('/planes/<id>/profile/cities', methods=['GET'])
def plane_profile_cities(id):
    global URL
    """
    Make sure you have created index on planesHistory collection
    db.planesHistory.createIndex({"plane":1,'date':1})
    """
    
    id = str(id)
    try:
        db = client.get_database(name='logistics')
        planes_col = db.get_collection(name='planesHistory')
        
        match = {'$match':{'plane': id}}
        addFields = {"$addFields":{"time":{"$dateToString":{"format":"%Y-%m-%d","date":"$date"}}}}
        groupby = {"$group":{"_id":{"time":"$time"},'cities':{'$addToSet':'$location'}}}
        sort = {"$sort":{"_id.time":1 }}
        project = {'$project':{'plane':id,'Date':'$_id.time','_id':0,'cities':1}}
        data = list(planes_col.aggregate([match, addFields, groupby, sort, project ]))
        
        return jsonify(data),200
    except Exception as e:
        return jsonify({"Error":str(e)}),400
    
    finally:
        client.close()

@app.route("/cargoes/<id>/profile/", methods=["GET"])
def cargo_profile(id):
    global URL
    id = ObjectId(id)
    try:
        db = client.get_database(name='logistics')
        cargos_col = db.get_collection(name='cargos')
        
        query = {"_id":id}
        cargo = cargos_col.find_one(query)
        if cargo is None:
            return jsonify(cargo),400
        cargo['id'] = str(cargo['_id'])
        cargo.pop('_id')
        return jsonify(cargo),200

    except Exception as e:
        print(e)
        return jsonify({"Error":str(e)}),400



@app.route('/avg_delivery/', methods = ['GET','POST'])
def avg_delivery():

    db = client.get_database(name='logistics')
    cargos_col = db.get_collection(name='cargos')
    deltime_col = db.get_collection(name='deltime',read_concern=ReadConcern(level='majority'),write_concern=WriteConcern(w='majority'))
    if str(request.method).lower() == 'post':
        data = request.get_json()
        prev_deltime = list(deltime_col.aggregate([{'$sort':{'_id':-1}},{'$limit':1}]))
        if len(prev_deltime)>0:
            prev_deltime = prev_deltime[-1]
            delivery_time = datetime.datetime.now()
            cargo_id = str(data['cargo_id'])
            delivery_sum = prev_deltime['delivery_sum']+ (delivery_time - prev_deltime['delivery_time']).total_seconds()
            insert_data = {'_id':prev_deltime['_id']+1,'delivery_time':delivery_time,'delivery_sum':delivery_sum,'cargo_id':str(cargo_id)}
            inserted_id = deltime_col.insert_one(insert_data).inserted_id
            print("count deliveries")
            print(inserted_id)
            return jsonify({'response':'Average time between deliveries {}'.format(str(delivery_sum)),'measured_in':'seconds'}),200
        else:
            cargo_id = data['cargo_id']
            delivery_time = datetime.datetime.now()
            delivery_sum = 0
            insert_data = {'_id':1,'delivery_time': delivery_time,'cargo_id':str(cargo_id),'delivery_sum':delivery_sum}
            inserted_id = deltime_col.insert_one(insert_data).inserted_id
            print("count deliveries")
            print(inserted_id)
            return jsonify({'response':'Average time between deliveries {}'.format(str(delivery_sum)),'measured_in':'seconds'}),200
            
    else:
        prev_deltime = list(deltime_col.aggregate([{'$sort':{'_id':-1}},{'$limit':1}]))[-1]
        
        if prev_deltime:
            return jsonify({'avg_time':str(prev_deltime['delivery_sum']/prev_deltime['_id'])}),200
        else:
            jsonify({'avg_time':0,'response':'Parcels were yet to deliver'}),200
            
            

    

# If we run this standalone not in gunicorn or similar
if __name__ == "__main__":
    app.run()
