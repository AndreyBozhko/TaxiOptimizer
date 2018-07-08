import sys
sys.path.append("/home/ubuntu/TaxiOptimizer/helpers/")

import os
import time, json
from app import app
from flask import jsonify, render_template, request
from flask_googlemaps import Map
import helpers
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
app.coords = []

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
        query = "SELECT spot_lat, spot_lon, vehicle_id, vehicle_pos FROM %s WHERE vehicle_id='%s' ORDER BY datetime" % (app.dbconfig["dbtable_stream"], vid)
        for entry in fetch_from_postgres(query):
            yield entry


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
    vid = request.args.get('vehicle_id', default='', type=str)
    if vid not in app.allowed_taxis:
        while True:
            vid = random.choice(app.allowed_taxis)
            if vid not in app.vid:
                break

    app.vid += [vid]
    app.coords += [get_spots(vid)]
    res = [gen.next() for gen in app.coords]

    return render_template("track.html",
                           APIkey=app.APIkey,
                           vid=app.vid,
                           taxiloc=[{"lat": rs[3][1], "lng": rs[3][0]} for rs in res],
                           spots=[[{"lat": el[0], "lng": el[1]} for el in zip(rs[0], rs[1])] for rs in res])


@app.route("/query")
def query():
    res = [gen.next() for gen in app.coords]
    return jsonify(vid=app.vid,
                   taxiloc=[{"lat": rs[3][1], "lng": rs[3][0]} for rs in res],
                   spots=[[{"lat": el[0], "lng": el[1]} for el in zip(rs[0], rs[1])] for rs in res])
