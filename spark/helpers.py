from datetime import datetime
from math import floor
import scipy.spatial
from scipy.spatial import KDTree
from math import radians, cos, sin, asin, sqrt



def determine_time_slot(time):
    try:
	dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    except:
        return -1
    return (dt.hour*60+dt.minute)/10



def determine_block_ids(lon, lat):
    # if not in Manhattan
    if abs(lon+74) > 0.24 or abs(lat-40.75) > 0.24:
	return -1, -1

    # size of large block is 0.005   degree lat/lon
    # size of small block is 0.00025 degree lat/lon
    corner = [(lon+74.25), (lat-40.5)]

    small_block_id = map(lambda x: floor(x/0.00025), corner)
    large_block_id = map(lambda x: x/20, small_block_id)
    small_block_id = map(lambda x: x%20, small_block_id)

    large_block_id = large_block_id[0]*100 + large_block_id[1]
    small_block_id = small_block_id[0]*20  + small_block_id[1]

    return int(large_block_id), int(small_block_id)



def determine_subblock_lonlat(block_id, subblock_id):
    corner = (-74.25, 40.75)
    id1, id2 = (block_id/100, block_id%100), (subblock_id/20, subblock_id%20)
    return [corner[i]+id1[i]*0.005+(id2[i]+0.5)*0.00025 for i in range(2)]



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
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000 # in meters



if __name__ == "__main__":
    print haversine(-73.94, 40.15, -73.9402, 40.15)
    print determine_block_ids(-74.03, 40.78)
