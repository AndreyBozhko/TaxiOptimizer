import sys
sys.path.append("/home/ubuntu/TaxiOptimizer/helpers/")

import os
import time, json
from app import app
from flask import jsonify, render_template, request
from flask_googlemaps import Map
import helpers
import psycopg2


# configure connection string for PostgreSQL database
app.dbconfig = helpers.parse_config('/home/ubuntu/TaxiOptimizer/config/postgresql.config')
app.conn_str = "host='%s' dbname='%s' user='%s' password='%s'" % (app.dbconfig["host"],
                                                                  app.dbconfig["dbname"],
                                                                  app.dbconfig["user"],
                                                                  app.dbconfig["password"])

# set default vehicle_id and the list of coordinates to display
app.vid = ''
app.coords = None

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
        query = "SELECT spot_lat, spot_lon, vehicle_id, vehicle_pos FROM %s WHERE vehicle_id='%s' ORDER BY datetime" % (app.dbconfig["dbtable_stream"], app.vid)
        for entry in fetch_from_postgres(query):
            yield entry


# get the set of valid taxi vehicle_ids
app.allowed_taxis = fetch_from_postgres("SELECT DISTINCT vehicle_id FROM %s" % app.dbconfig["dbtable_stream"])
app.allowed_taxis = set([x[0] for x in app.allowed_taxis])


# define the behavior when accessing routes '/', '/index', '/demo', '/track' and '/query'

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', APIkey=app.APIkey)


@app.route('/demo')
def demo():
    return render_template('demo.html', APIkey=app.APIkey)


@app.route("/track")
def track():
    app.vid = request.args.get('vehicle_id', default='', type=str)
    if app.vid not in app.allowed_taxis:
        app.vid = app.allowed_taxis.pop()
        app.allowed_taxis.add(app.vid)

    app.coords = get_spots(app.vid)
    res = app.coords.next()
    return render_template("track.html",
                           APIkey=app.APIkey,
                           vid=app.vid,
                           taxiloc={"lat": res[3][1], "lng": res[3][0]},
                           spots=[{"lat": el[0], "lng": el[1]} for el in zip(res[0], res[1])])


@app.route("/query")
def query():
    res = app.coords.next()
    return jsonify(vid=app.vid,
                   taxiloc={"lat": res[3][1], "lng": res[3][0]},
                   spots=[{"lat": el[0], "lng": el[1]} for el in zip(res[0], res[1])])
