import sys
import pyspark
from streamers import TaxiStreamer



if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write("Usage: spark-submit --packages <packages> stream_data.py <kafkaconfigfile> <schemaconfigfile> <postgresconfigfile> \n")
        sys.exit(-1)

    kafka_configfile, schema_configfile, psql_configfile = sys.argv[1:4]

    streamer = TaxiStreamer(kafka_configfile, schema_configfile, psql_configfile, 1, 0)
    streamer.run()
