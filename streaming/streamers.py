import sys
sys.path.append("./helpers/")

import json
import pyspark
import helpers
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


####################################################################

class SparkStreamerFromKafka:

    def __init__(self, kafka_configfile, schema_configfile, batch_interval, start_offset):
        """
        constructor
        """
        self.sc  = pyspark.SparkContext().getOrCreate()
        self.scc = pyspark.streaming.StreamingContext(self.sc, batch_interval)

        self.sc.setLogLevel("ERROR")

        self.kafka_config = helpers.parse_config(kafka_configfile)
        self.kafka_config = helpers.replace_envvars_with_vals(self.kafka_config)
        self.schema       = helpers.parse_config(schema_configfile)

        self.initializeStream(start_offset)


    def initializeStream(self, start_offset):
        """
        initializes stream
        """
        topic, n = self.kafka_config["TOPIC"], self.kafka_config["PARTITIONS"]
        try:
            fromOffsets = {TopicAndPartition(topic, i): long(start_offset) for i in range(n)}
        except:
            fromOffsets = None

        self.dataStream = KafkaUtils.createDirectStream(self.scc, [topic],
                                            {"metadata.broker.list": self.kafka_config["BROKERS_IP"]},
                                            fromOffsets=fromOffsets)


    def processStream(self):
        """
        cleans the streamed data
        """
        self.dataStream = (self.dataStream
                                    .map(lambda x: json.loads(x[1]))
                                    .map(helpers.add_block_fields)
                                    .map(helpers.add_time_slot_field)
                                    .filter(lambda x: x is not None))


    def run(self):
        """
        starts streaming
        """
        self.processStream()
        self.scc.start()
        self.scc.awaitTermination()


####################################################################

class TaxiStreamer(SparkStreamerFromKafka):

    def __init__(self, kafka_configfile, schema_configfile, psql_configfile, batch_interval=1, start_offset=None):
        """
        constructor
        """
        SparkStreamerFromKafka.__init__(self, kafka_configfile, schema_configfile, batch_interval, start_offset)
        self.psql_config = helpers.get_psql_config(psql_configfile)


    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database
        """
        sqlContext = pyspark.sql.SQLContext(self.sc)
        return (sqlContext.read.jdbc(url      =self.psql_config["url"],
                                    table     =self.psql_config["dbtable"],
                                    properties=self.psql_config["properties"])
                               .rdd.map(lambda x: x.asDict())
                               .map(lambda x: ((x["time_slot"], x["block_idx"], x["block_idy"]),
                                               (x["longitude"], x["latitude"], x["passengers"]))))


    def processStream(self):
        """
        processes stream
        """
        sql_data = self.load_batch_data()
        sql_data.persist(pyspark.StorageLevel.MEMORY_ONLY)

        SparkStreamerFromKafka.processStream(self)


        def process(time, rdd):
            global iPass
            try:
                iPass += 1
            except:
                iPass = 1

            print("========= Microbatch Number: {0} - {1} =========".format(iPass, str(time)))

            try:
                rdd_bcast = (rdd.map(lambda x: ((x["time_slot"], x["block_id_x"], x["block_id_y"]),
                                                (x["vehicle_id"], x["longitude"], x["latitude"])))
                                .groupByKey().collect())
                if len(rdd_bcast) == 0:
                    return

                rdd_bcast = self.sc.broadcast({x[0]:x[1] for x in rdd_bcast})

                def func(x):
                    try:
                        return map(lambda el: (el, x[1]), rdd_bcast.value[x[0]])
                    except:
                        return [None]

                resDF = sql_data.flatMap(func).filter(lambda x: x is not None)
                print resDF.count()

            except:
                pass

        self.dataStream.foreachRDD(process)
