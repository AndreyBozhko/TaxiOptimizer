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
    dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
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



def get_neighboring_blocks(bl):
    """
    returns list of block_id for blocks that are adjacent to block bl
    (assuming 100x100 block grid)
    :type bl: int
    :rtype : list[int]
    """
    return [x+bl for x in [-1, 1, -100, 100, -99, 99, -101, 101]]



def get_blocks_with_timeslots(bl, ts):
    """
    returns list of (block_id, time_slot) for blocks that are adjacent to block bl
    time_slot is ts for block bl, and (ts+1) for adjacent blocks
    time_slot is in range(0, 144)
    :type bl: int
    :type ts: int
    :rtype : list[(int, int)]
    """
    return [(bl, ts)] + [(b, (ts+1)%144) for get_neighboring_blocks(bl) if b >= 0 and (b < 100**2)]



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



def get_top_spots(taxi, sql_data):
    """

    :type taxi: dict
    :rtype : (str, list[int])
    """
    #sql_data.createOrReplaceTempView("sql_data")

    res = []
    for bl, ts in get_blocks_with_timeslots(taxi["block_id"], taxi["time_slot"]):
        res += [(sql_data.select("subblocks_psgcnt")
                .where("block_id=".format(bl))
                .where("time_slot=".format(ts))
                .collect()), bl]
    res = sorted(res, key=lambda el: -el[0][1])[:10]

    return [determine_subblock_lonlat(res[1], res[0][0]) for el in res]



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
        #msg = json.dumps(msg)
    except:
        return
    return msg



def add_block_fields(record):
    """

    """
    try:
        lon, lat = [float(record[field]) for field in ["longitude", "latitude"])
        record["block_id"], record["sub_block_id"] = determine_block_ids(lon, lat)
    except:
        return
    if record["block_id"] < 0:
        return
    return dict(record)



ef add_time_slot_field(record):
    """

    """
    try:
        record["time_slot"] = determine_time_slot(record["datetime"])
    except:
        return
    if record["time_slot"] < 0:
        return
    return dict(record)



# def clean_raw_data(msg, schema):
#     """
#     cleans the message msg, leaving only the fields given by schema
#     :type msg: str
#     :type schema: dict
#     :rtype : dict
#     """
#     fields = msg.split(schema["DELIMITER"])
#     res = {}
#
#     try:
#
#         lon, lat, psg, dt = map(lambda name: fields[schema["FIELDS"][name]],
#                                 ["longitude", "latitude", "passengers", "datetime"])
#
#         lon, lat = map(float, [lon, lat])
#         res["passengers"] = int(psg)
#         res["time_slot"] = determine_time_slot(dt)
#         res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)
#
#     except:
#         return
#
#     if res["block_id"] < 0 or res["time_slot"] < 0:
#         return
#
#     return res



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
            if type(val) is unicode and len(val) > 0 and val[0] == '$':
                dic[el] = os.getenv(val[1:])
    return dic



# def map_func(key):
#     for line in key.get_contents_as_string().splitlines():
#         yield line.strip()
