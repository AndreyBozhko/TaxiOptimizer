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



def determine_block_id(lon, lat):
    if abs(lon+74) > 0.24 or abs(lat-40.75) > 0.24:
	return -1
    return floor((lon+74.25)*200)*100 + floor((lat-40.5)*200)



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



def determine_top_spots(collection):
    
    radius = 0.0005 # in longitude/latitude

    arr = [[el["longitude"], el["latitude"]] for el in collection]
    if arr == []:
        return []

    tree = KDTree(arr)
    result = sorted(map(lambda pt: (len(tree.query_ball_point(pt[0], radius)), pt[1]), zip(arr, range(len(arr)))), key=lambda x: -x[0])

    clusters = [arr[result[0][1]]]   

    for pt in map(lambda x: arr[x[1]], result):
        if not True in map(lambda p: scipy.spatial.distance.euclidean(p, pt) <= radius, clusters):
            clusters.append(pt)
        if len(clusters) == 10:
            break
    
    return clusters[:]



if __name__ == "__main__":
    print haversine(-73.94, 40.15, -73.9402, 40.15)
