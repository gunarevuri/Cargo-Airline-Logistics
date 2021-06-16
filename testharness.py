#!/usr/bin/python3
import requests 
import json
import datetime
import math
import unit_tests
import time
from pprint import pprint
import random

city_locations = {}

#This is also in unit_tests

url = "http://localhost:5000"

def run_unit_tests():
    unit_tests.city_unit_tests()
    unit_tests.plane_unit_tests()
    unit_tests.cargo_unit_tests()


#Land when we get close to an airport
# Unload and deliver anythign bound for here
# Keep anything bound for somethere on our route
# Unload and unown anything else

def LandPlane(plane,destination,destLocation):

    print('Landing %s at %s'  % (plane['callsign'],destination))
    plane['currentLocation']=destLocation
    plane['heading']=0
    
      
    requests.delete("%s/planes/%s/route/destination" % (url,plane['callsign']))
    requests.put( "%s/planes/%s/location/%f,%f/%d/%s"  % (url,plane['callsign'],
    plane['currentLocation'][0], plane['currentLocation'][1],plane['heading'],destination))

    cargoes =  requests.get("%s/cargo/location/%s" % (url,plane['callsign'])).json()

    #Unload everything when we land - and remove it's courier unless it's headed to somewhere we 
    #are routed to
   
    if  cargoes != None :
        for cargo in cargoes:
            if cargo['destination'] == destination:
                print("Unloading delivered package %s" % (cargo['id']))
                requests.put("%s/cargo/%s/location/%s" % (url,cargo['id'],destination))
                requests.put("%s/cargo/%s/delivered" % (url,cargo['id']))
                
            else:
                if cargo['destination'] in plane['route'] :
                    print("Keeping cargo %s onboard for delivery to %s" % (cargo['id'],cargo['destination']))
                else:
                    requests.delete("%s/cargo/%s/courier" % (url,cargo['id']))
                    requests.put("%s/cargo/%s/location/%s" % (url,cargo['id'],destination))
                    print("Offloading %s" % cargo['id'])


    #Flag as delivered if this is the final destination
    #Mark as uncouriered 
    #Take on any Cargo for me
    sitecargo =  requests.get("%s/cargo/location/%s" % (url,destination)).json()

    if sitecargo != None:
        for cargo in sitecargo:
            if cargo['courier'] == plane['callsign']:
                print("On Loading %s for %s" % (cargo['id'],cargo['destination']))
                requests.put("%s/cargo/%s/location/%s" % (url,cargo['id'], plane['callsign']))


def roundtwo(n):
    return int(n*100)/100;

def movePlane(plane,destination):

    if destination == None:
         return
    currentLocation = plane['currentLocation']
           
    destLocation = city_locations[destination]
    #print(`${plane.callsign} ${plane.currentLocation} going to ${destination} ${destLocation}`);

    dx = destLocation[0]-currentLocation[0]
    dy = destLocation[1]-currentLocation[1]

    if abs(dy) == 0.0:
        dy = 0.1 
    if abs(dx) < 0.5 and abs(dy) < 0.5:
       #console.log(`${plane.callsign} ${plane.currentLocation} going to ${destination} ${destLocation}`);
       LandPlane(plane,destination,destLocation)   
    else:
        angle = math.atan(dx/dy)*180/math.pi;
        if dy < 0:
             angle = angle  + 180
        if angle < 0:
            angle = angle + 360
        #Calculate new position*
        dLon = math.sin(angle*math.pi/180)
        dLat = math.cos(angle*math.pi/180)
        newval = [roundtwo(plane['currentLocation'][0]+dLon),roundtwo(plane['currentLocation'][1]+dLat)];
        plane['currentLocation']=newval
        plane['heading'] = math.floor(angle);
        #TODO Teach planes the world is round :0)
        r = requests.put("%s/planes/%s/location/%f,%f/%d" % (url,plane['callsign'],plane['currentLocation'][0],
        plane['currentLocation'][1],plane['heading']))

    



def run_simulation():
    #Move planes and set heading
    cities = requests.get(url + "/cities").json()
    for city in cities:
        city_locations[city['name']] = city['location']

    while True:
        planes = requests.get(url + "/planes").json()
        for plane in planes:
            if len(plane["route"]) > 0:
                destination = plane["route"][0]
                movePlane(plane,destination);

        #New cargo arrives randomly
        arrive_at = random.choice(list(city_locations.keys()))
        send_to = random.choice(list(city_locations.keys()))

        if arrive_at != send_to:
            cargoes =  requests.get("%s/cargo/location/%s" % (url,arrive_at)).json()
            
            if len(cargoes) < 20 :
                    print("Parcel arrives at %s for %s, %d pending deliviery" % (arrive_at,send_to,len(cargoes)+1))
                    print(requests.post("%s/cargo/%s/to/%s" % (url,arrive_at,send_to)).json())
            time.sleep(0.5)


if __name__ == "__main__":
   run_unit_tests()
   run_simulation()
