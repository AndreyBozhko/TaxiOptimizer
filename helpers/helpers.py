import os
import math
import json
import subprocess
from datetime import datetime



def determine_time_slot(time):
    """
    determines time slot of the day based on given datetime
    :type time: str
    :rtype : int
    """
    dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (dt.hour*60+dt.minute)/10



def determine_block_ids(lon, lat):
    """
    calculates ids of blocks/subblocks based on given coordinates
    :type lon: float
    :type lat: float
    :rtype : [(int, int), (int, int)]
    """
    # if not in Manhattan
    #if abs(lon+74) > 0.24 or abs(lat-40.75) > 0.24:
    #    return -1, -1

    # size of large block is 0.005   degree lat/lon
    # size of small block is 0.00025 degree lat/lon
    corner = [(lon+74.25), (lat-40.5)]

    small_block_id = map(lambda x: int(math.floor(x/0.00025)), corner)
    large_block_id = map(lambda x: x/20, small_block_id)
    #small_block_id = map(lambda x: x%20, small_block_id)

    #large_block_id = large_block_id[0]*100 + large_block_id[1]
    #small_block_id = small_block_id[0]*20  + small_block_id[1]

    return tuple(large_block_id), tuple(small_block_id)



def get_neighboring_blocks(bl):
    """
    returns list of block_id for blocks that are adjacent to block bl
    :type bl: int
    :rtype : list[(int, int)]
    """
    return [(bl[0]+i, bl[1]+j) for i in [-1,0,+1] for j in [-1,0,+1] if not (i == 0 and j == 0)]



def determine_subblock_lonlat(subblock_id):
    """
    calculates coordinates of the center of a subblock based on block_id and subblock_id
    :type block_id: int
    :type subblock_id: int
    :rtype : (float, float)
    """
    corner = (-74.25, 40.5)
    return [corner[i]+(subblock_id[i]+0.5)*0.00025 for i in range(2)]



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



def map_schema(line, schema):
    """
    cleans the message msg, leaving only the fields given by schema
    :type line: str
    :type schema: dict
    :rtype : dict
    """
    try:
        msg = line.split(schema["DELIMITER"])
        msg = {key:msg[schema["FIELDS"][key]] for key in schema["FIELDS"].keys()}
    except:
        return
    return msg



def add_block_fields(record):
    """
    adds fields block_id (tuple), sub_block_id (tuple), block_id_x (int), block_id_y (int)
    to the record based on existing fields longitude and latitude
    :type record: dict
    :rtype : dict
    """
    try:
        lon, lat = [float(record[field]) for field in ["longitude", "latitude"]]
        record["block_id"], record["sub_block_id"] = determine_block_ids(lon, lat)
        record["block_id_x"], record["block_id_y"] = record["block_id"]
    except:
        return
    return dict(record)



def add_time_slot_field(record):
    """
    adds field time_slot(int) to the record based on existing field datetime
    :type record: dict
    :rtype : dict
    """
    try:
        record["time_slot"] = determine_time_slot(record["datetime"])
    except:
        return
    #if record["time_slot"] < 0:
    #    return
    return dict(record)



def check_passengers(record):
    """
    converts field passengers from str to int and check if its value is >= 1
    :type record: dict
    :rtype : dict
    """
    try:
        record["passengers"] = int(record["passengers"])
    except:
        return
    if record["passengers"] < 1:
        return
    return dict(record)



def parse_config(configfile):
    """
    reads configs saved as json record in configuration file and returns them
    :type configfile: str
    :rtype : dict
    """
    return json.load(open(configfile, "r"))



def get_psql_config(psql_configfile):
    """
    returns configurations of the PostgreSQL table
    :type psql_configfile: str
    :rtype : dict
    """
    config = parse_config(psql_configfile)
    config = replace_envvars_with_vals(config)

    config["url"] = "{}{}:{}/{}".format(config["urlheader"],
                                        config["host"],
                                        config["port"],
                                        config["database"])

    return config



def replace_envvars_with_vals(dic):
    """
    for a dictionary dic which contains values of the form "$varname",
    replaces such values with the values of corresponding environmental variables
    :type dic: dict
    :rtype : dict
    """
    for el in dic.keys():
        val = dic[el]
        if type(val) is dict:
            val = replace_envvars_with_vals(val)
        else:
            if type(val) in [unicode, str] and len(val) > 0 and val[0] == '$':
                #dic[el] = os.getenv(val[1:])
                command = "echo {}".format(val)
                dic[el] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().strip()
    return dic
