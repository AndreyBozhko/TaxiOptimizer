import os
import math
import json
from datetime import datetime



def determine_time_slot(time):
    """
    determines time slot of the day based on given datetime
    :type time: str
    :rtype : int
    """
    try:
        dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    except:
        return -1
    return (dt.hour*60+dt.minute)/10



def determine_block_ids(lon, lat):
    """
    calculates ids of blocks/subblocks based on given coordinates
    :type lon: float
    :type lat: float
    :rtype : (int, int)
    """
    # if not in Manhattan
    if abs(lon+74) > 0.24 or abs(lat-40.75) > 0.24:
        return -1, -1

    # size of large block is 0.005   degree lat/lon
    # size of small block is 0.00025 degree lat/lon
    corner = [(lon+74.25), (lat-40.5)]

    small_block_id = map(lambda x: int(math.floor(x/0.00025)), corner)
    large_block_id = map(lambda x: x/20, small_block_id)
    small_block_id = map(lambda x: x%20, small_block_id)

    large_block_id = large_block_id[0]*100 + large_block_id[1]
    small_block_id = small_block_id[0]*20  + small_block_id[1]

    return large_block_id, small_block_id



def determine_subblock_lonlat(block_id, subblock_id):
    """
    calculates coordinates of the center of a subblock based on block_id and subblock_id
    :type block_id: int
    :type subblock_id: int
    :rtype : (float, float)
    """
    corner = (-74.25, 40.5)
    id1, id2 = (block_id/100, block_id%100), (subblock_id/20, subblock_id%20)
    return [corner[i]+id1[i]*0.005+(id2[i]+0.5)*0.00025 for i in range(2)]



# def haversine(lon1, lat1, lon2, lat2):
#     """
#     Calculate the great circle distance between two points
#     on the earth (specified in decimal degrees)
#     """
#     # convert decimal degrees to radians
#     lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
#
#     # haversine formula
#     dlon = lon2 - lon1
#     dlat = lat2 - lat1
#     a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#     c = 2 * asin(sqrt(a))
#     r = 6371 # Radius of earth in kilometers. Use 3956 for miles
#     return c * r * 1000 # in meters


def clean_data(msg, schema):
    """
    cleans the message msg, leaving only the fields given by schema
    :type msg: str
    :type schema: dict
    :rtype : dict
    """
    fields = msg.split(',')
    res = {}

    lon, lat, psg, dt = map(lambda name: fields[schema[name]],
                            ["longitude", "latitude", "passengers", "datetime"])

    try:
        lon, lat = map(float, [lon, lat])
        res["passengers"] = int(psg)
        res["time_slot"] = determine_time_slot(dt)
        res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)
    except:
        return

    if res["block_id"] < 0 or res["time_slot"] < 0:
        return

    return res


def parse_config(configfile):
    """
    reads configs saved as json record in configuration file and returns them
    :type configfile: str
    :rtype : dict
    """
    return json.load(open(configfile, "r"))


def replace_envvars_with_vals(dic):
    """
    for a dictionary dic which contains values of the form "$varname",
    replaces such values with the values of corresponding environmental variables
    """
    for el in dic.keys():
        val = dic[el]
        if type(val) is dict:
            val = replace_envvars_with_vals(val)
        else:
            if type(val) is unicode and len(val) > 0 and val[0] == '$':
                dic[el] = os.getenv(val[1:])
    return dic
