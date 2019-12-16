import urllib.request, json 
from pandas.io.json import json_normalize
from geopy.geocoders import Nominatim

def getResponse(url):
    operUrl = urllib.request.urlopen(url)
    if(operUrl.getcode()==200):
        data = operUrl.read()
        jsonData = json.loads(data)
    else:
        print("Error receiving data", operUrl.getcode())
    return jsonData

def apiNormalizer(jsons, features):
    normals = json_normalize(jsons[str(features)])
    return normals

def get_address(loct):
    geolocator = Nominatim(user_agent="waze")
#     latlong = str(lat)+","+str(long)
    latlong = str(loct)
    location = geolocator.reverse(latlong)
    return location.raw

def get_fulladdress(loct):
    geolocator = Nominatim(user_agent="waze")
#     latlong = str(lat)+","+str(long)
    latlong = str(loct)
    location = geolocator.reverse(latlong)
    return location.address