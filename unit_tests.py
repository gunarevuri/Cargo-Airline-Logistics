import requests 
import json
import datetime

url = "http://localhost:5000"

#Die on any error
def fail(what):
    print("ERROR :" + what)
    exit(1)

#Generic checking of return format
def test_response(response,status,checktype,template):
    if response.status_code != status:
        fail("Incorrect status expected %d got %d" % (status,response.status_code))
    
    if response.status_code != 200:
        return

    try:
        rval = response.json()
    except Exception as e :
        fail("Failed to parse JSON " + str(e))
    
    if checktype != None:
        if isinstance(rval,checktype) == False:
            fail("Returned data type was %s not %s " % (str(type(rval)),str(checktype)))

        #If we get a list check the first object otherwise check the object
        if template != None:
            if checktype == list:
                if len(rval) < 1: 
                    fail("Empty list returned")
                obj = rval[0]
            else:
                obj = rval


            for (fname,ftype) in template.items():
                try:
                    val = obj[fname]
                    if isinstance(val,ftype) == False and val != None:
                        fail("Returned data field %s was %s not %s " % (fname,str(type(val)),str(ftype)))
                except Exception as e:
                    fail("Failed to get key %s from object %s" % (fname,obj))
                   


def city_unit_tests():
    cityfields = { "name" : str, "country":str, "location": list}

    #Get All City Info
    #@app.route("/cities", methods=["GET"])

    print("Testing /cities")
    r = requests.get(url + "/cities")
    test_response(r,200,list,cityfields)

    #@app.route("/cities/<id>", methods=["GET"])

    print("Testing /cities/London")
    r = requests.get(url + "/cities/London")
    test_response(r,200,dict,cityfields)

    print("Testing /cities/AnkhMorpork")
    r = requests.get(url + "/cities/AnkhMorpork")
    test_response(r,404,dict,cityfields)



def plane_unit_tests():
    
    planefields = { "callsign" : str, "heading": float, "currentLocation": list, "route" : list, "landed": str}
    print("Testing GET /planes")
    r = requests.get(url + "/planes")
    test_response(r,200,list,planefields)

    print("Testing GET /planes/CARGO0")
    r = requests.get(url + "/planes/CARGO0")
    test_response(r,200,dict,planefields)

    print("Testing GET /planes/AIRWOLF")
    r = requests.get(url + "/planes/AIRWOLF")
    test_response(r,404,dict,planefields)

    print("Testing PUT /planes/CARGO0/location/5.5,56.5/180")
    r = requests.put(url + "/planes/CARGO0/location/5.5,56.5/180")
    test_response(r,200,None,planefields)
    r = requests.get(url + "/planes/CARGO0")
    test_response(r,200,dict,planefields)
    o = r.json()
    if o.get("currentLocation") != [5.5,56.5]:
        fail("Incorrectly recorded position")
    if o.get("heading") != 180:
        fail("Incorrectly recorded heading")


    print("Testing PUT /planes/CARGO0/location/5.5,56.5/180/Edinburgh")
    r = requests.put(url + "/planes/CARGO0/location/5.5,56.5/180/Edinburgh")
    test_response(r,400,None,planefields)


    print("Testing PUT /planes/CARGO0/location/5.5,56.5/180/Madrid")
    r = requests.put(url + "/planes/CARGO0/location/5.5,56.5/180/Madrid")
    test_response(r,200,None,planefields)
    r = requests.get(url + "/planes/CARGO0")
    test_response(r,200,dict,planefields)
    o = r.json()
    if o.get("currentLocation") != [5.5,56.5]:
        fail("Incorrectly recorded position")
    if o.get("heading") != 180:
        fail("Incorrectly recorded heading")
    if o.get("landed") != "Madrid":
        fail("Incorrectly recorded landed")


    print("Testing PUT /planes/CARGO1/route/Gondor")
    r = requests.put(url + "/planes/CARGO1/route/Gondor")
    test_response(r,400,None,planefields)

    print("Testing PUT /planes/CARGO1/route/Paris")
    r = requests.put(url + "/planes/CARGO1/route/Paris")
    test_response(r,200,None,planefields)
    r = requests.get(url + "/planes/CARGO1")
    test_response(r,200,dict,planefields)
    o = r.json()
    if o.get("route") != ["Paris"]:
         fail("Incorrectly replaced route")

    print("Testing POST /planes/CARGO1/route/Berlin")
    r = requests.post(url + "/planes/CARGO1/route/Berlin")
    test_response(r,200,None,planefields)
    r = requests.get(url + "/planes/CARGO1")
    test_response(r,200,dict,planefields)
    o = r.json()
    if o.get("route") != ["Paris","Berlin"]:
         fail("Incorrectly added to route")

    print("Testing POST /planes/CARGO1/route/Atlantis")
    r = requests.post(url + "/planes/CARGO1/route/Atlantis")
    test_response(r,400,None,planefields)

    print("Testing DELETE /planes/CARGO1/route/destination")
    r = requests.delete(url + "/planes/CARGO1/route/destination")
    test_response(r,200,None,planefields)

    r = requests.get(url + "/planes/CARGO1")
    test_response(r,200,dict,planefields)
    o = r.json()
    if o.get("route") != ["Berlin"]:
         fail("Failed to delete destination")

def cargo_unit_tests():
    cargofields = { "id": str,"destination": str,
            "location":str,"courier": str,
            "received": str, "status": str }

    #Check we have at least one
    print("Testing POST /cargo/Berlin/to/London")
    r = requests.post(url + "/cargo/Berlin/to/London")
    test_response(r,200,None,None)
    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    id = o.get("id")
    if id == None:
        fail("No id value returned for new cargo")

    print("Testing GET /cargo/location/Berlin")
    r = requests.get(url + "/cargo/location/Berlin")
    test_response(r,200,list,cargofields)
    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    
    found = None
    for c in o:
        if c.get("id") == id and id != None:
            found = c

    if found == None:
        fail("Newly added Cargo not present")

    print("Testing PUT /cargo/%s/courier/CARGO0" % id)
    r = requests.put("%s/cargo/%s/courier/CARGO0" % (url,id))
    test_response(r,200,None,cargofields)
    
    print("Testing GET /cargo/location/Berlin")
    r = requests.get(url + "/cargo/location/Berlin")
    test_response(r,200,list,cargofields)
    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    
    found = None
    for c in o:
        if c.get("id") == id and id != None:
            found = c

    if found == None:
        fail("Updated Cargo not present")

    if found.get("courier") != "CARGO0":
        fail("Failed to set Courier on cargo")

    print("Testing DELETE /cargo/%s/courier" % id)
    r = requests.delete("%s/cargo/%s/courier" % (url,id))
    test_response(r,200,None,cargofields)
    
    print("Testing GET /cargo/location/Berlin")
    r = requests.get(url + "/cargo/location/Berlin")
    test_response(r,200,list,cargofields)
    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    
    found = None
    for c in o:
        if c.get("id") == id and id != None:
            found = c

    if found == None:
        fail("Updated Cargo not present")

    if found.get("courier") != None:
        fail("Failed to remove Courier from cargo")

    #Teleporting must be allowed for this
    print("Testing PUT /cargo/%s/location/CARGO1" % id)
    r = requests.put("%s/cargo/%s/location/CARGO1" % (url,id))
    test_response(r,200,None,cargofields)

    print("Testing GET /cargo/location/CARGO1")
    r = requests.get(url + "/cargo/location/CARGO1")
    test_response(r,200,list,cargofields)
    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    
    found = None
    for c in o:
        if c.get("id") == id and id != None:
            found = c

    if found == None:
        fail("Updated Cargo not present")

    if found.get("location") != "CARGO1":
        fail("Failed to relocate cargo")


    #sDelivered is shoudl hide from API
    print("Testing PUT /cargo/%s/delivered" % id)
    r = requests.put("%s/cargo/%s/delivered" % (url,id))
    test_response(r,200,None,cargofields)

    print("Testing GET /cargo/location/CARGO1")
    r = requests.get(url + "/cargo/location/CARGO1")

    try:
        o = r.json()
    except Exception as e:
        fail("Could not parse returned JSON")
    
    found = None
    for c in o:
        if c.get("id") == id and id != None:
            found = c

    if found != None:
        fail("Delivered cargo still visible")