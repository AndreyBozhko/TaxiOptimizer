import sys
sys.path.append("/home/ubuntu/TaxiOptimizer/helpers/")

import os
import time, json
from app import app
from datetime import datetime
from flask import jsonify, render_template, request
import helpers
from math import floor
from more_itertools import peekable
import psycopg2
import random


# configure connection string for PostgreSQL database
app.dbconfig = helpers.parse_config('/home/ubuntu/TaxiOptimizer/config/postgresql.config')
app.conn_str = "host='%s' dbname='%s' user='%s' password='%s'" % (app.dbconfig["host"],
                                                                  app.dbconfig["dbname"],
                                                                  app.dbconfig["user"],
                                                                  app.dbconfig["password"])

# set default vehicle_id and the list of coordinates to display
app.vid = []
app.res = []
app.coords = []

# time will be in the range from 10am to 10pm
app.curtime = 600


def print_time(t):
    """
    prints time t in "11:23 AM" format
    :type t: float      time in minutes
    :rtype : str        time in specified format
    """
    dt = datetime(2014, 8, 1, int(t/60), int(t%60), 0)
    return dt.strftime("%I:%M %p")


# read Google Maps API Key from file
with open("/home/ubuntu/TaxiOptimizer/config/GoogleAPIKey.config") as f:
    app.APIkey = f.readline().strip()


def fetch_from_postgres(query):
    """
    gets the data from PostgreSQL database using the specified query
    :type query: str
    :rtype     : list of records from database
    """
    conn = psycopg2.connect(app.conn_str)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def get_spots(vid):
    """
    yields next pickup spots for taxi with given vid
    :type vid: str
    :rtype   : generator
    """
    while True:
        query = "SELECT spot_lat, spot_lon, vehicle_id, vehicle_pos, datetime FROM %s WHERE vehicle_id='%s' ORDER BY datetime" % (app.dbconfig["dbtable_stream"], vid)
        for entry in fetch_from_postgres(query):
            entr = list(entry)
            dt = datetime.strptime(entry[-1], "%Y-%m-%d %H:%M:%S")
            entr[-1] = (dt.hour*60+dt.minute)+(dt.day-1)*1440
            yield entr


def get_next(coords, res=None):
    """
    from list of spots generators coords and list of previous spots,
    produces a list of current spots
    :type coords: list[generator]       list of peekable generators
    :type res   : list[spot]            spot schema is defined by query in get_spots(vid)
    :rtype      : list[spot]
    """
    if res == None:
        res = [c.next() for c in coords]
        if len(coords) == 1 or app.curtime >= 1320:
            app.curtime = 590
        return res

    for i, c in enumerate(coords):
        if res[i][-1] > app.curtime:
            continue
        while res[i][-1] < app.curtime:
            if c.peek()[-1] >= res[i][-1]:
                res[i] = c.peek()
                c.next()
            else:
                break
    return res



# get the set of valid taxi vehicle_ids
app.allowed_taxis = fetch_from_postgres("SELECT DISTINCT vehicle_id FROM %s" % app.dbconfig["dbtable_stream"])
app.allowed_taxis = [x[0] for x in app.allowed_taxis]



# define the behavior when accessing routes '/', '/index', '/demo', '/track' and '/query'

@app.route('/')
@app.route('/index')
def index():
    app.vid, app.coords = [], []
    return render_template('index.html', APIkey=app.APIkey)



@app.route('/demo')
def demo():
    return render_template('demo.html', APIkey=app.APIkey)



@app.route("/track")
def track():
    newvid = []
    vids = request.args.get('vehicle_id', default='', type=str).split(',')
    for vid in vids:
        if vid not in app.allowed_taxis:
            while True:
                v = random.choice(app.allowed_taxis)
                if v not in app.vid:
                    break
        else:
            v = vid
        newvid += [v]

    app.vid += newvid
    app.coords += [peekable(get_spots(vid)) for vid in newvid]

    app.res = get_next(app.coords)
    return render_template("track.html",
                           APIkey=app.APIkey,
                           vid=app.vid,
                           timestr=print_time(app.curtime),
                           taxiloc=[{"lat": rs[3][1], "lng": rs[3][0]} for rs in app.res],
                           corners=[(floor(rs[3][1]*200)/200.0, floor(rs[3][0]*200)/200.0) for rs in app.res],
                           spots=[[{"lat": el[0], "lng": el[1]} for el in zip(rs[0], rs[1])] for rs in app.res])



@app.route("/query")
def query():
    app.curtime += 0.5

    if app.curtime > 1320:
        app.coords = [peekable(get_spots(vid)) for vid in app.vid]
        app.res = get_next(app.coords)
    else:
        app.res = get_next(app.coords, app.res)

    return jsonify(vid=app.vid,
                   timestr=print_time(app.curtime),
                   taxiloc=[{"lat": rs[3][1], "lng": rs[3][0]} for rs in app.res],
                   corners=[(floor(rs[3][1]*200)/200.0, floor(rs[3][0]*200)/200.0) for rs in app.res],
                   spots=[[{"lat": el[0], "lng": el[1]} for el in zip(rs[0], rs[1])] for rs in app.res])
