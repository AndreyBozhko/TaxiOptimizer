import sys
sys.path.append("./helpers/")

import json
import pyspark
import helpers
from pyspark.streaming.kafka import KafkaUtils


####################################################################

class SparkStreamerFromKafka:

    def __init__(self, kafka_configfile, schema_configfile):
        """
        constructor
        """
        batch_interval = 0.1   # 0.1 second
        self.sc  = pyspark.SparkContext().getOrCreate()
        self.scc = pyspark.streaming.StreamingContext(self.sc, batch_interval)

        self.kafka_config = helpers.parse_config(kafka_configfile)
        self.schema       = helpers.parse_config(schema_configfile)


    def initializeStream(self):
        """
        initializes stream
        """
        self.dataStream = KafkaUtils.createStream(self.scc,
                                       self.kafka_config["ZOOKEEPER_IP"],
                                       "spark-streaming-consumer",
                                       {self.kafka_config["TOPIC"]: self.kafka_config["PARTITIONS"]})


    def processStream(self):
        """
        cleans the streamed data
        """
        self.dataStream = (self.dataStream
                                    .map(lambda x: json.loads(x[1]))
                                    .map(helpers.add_block_fields)
                                    .map(helpers.add_time_slot_field)
                                    .filter(lambda x: x is not None)
                                    .map(json.dumps))


    def run(self):
        """
        executes init and starts streaming
        """
        self.initializeStream()
        self.processStream()
        self.scc.start()
        self.scc.awaitTermination()


####################################################################

class TaxiStreamer(SparkStreamerFromKafka):

    def __init__(self, kafka_configfile, schema_configfile, psql_configfile):
        """
        constructor
        """
        SparkStreamerFromKafka.__init__(self, kafka_configfile, schema_configfile)
        self.psql_config = helpers.get_psql_config(psql_configfile)


    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database
        """
        sqlContext = pyspark.sql.SQLContext(self.sc)
        return sqlContext.read.jdbc(url       =self.psql_config["url"],
                                    table     =self.psql_config["dbtable"],
                                    properties=self.psql_config["properties"])


    def processStream(self):
        """
        processes stream
        """

        # create singleton Spark Session Instance for each RDD
        def getSparkSessionInstance(sparkConf):
            if ('sparkSessionSingletonInstance' not in globals()):
                globals()['sparkSessionSingletonInstance'] = (pyspark.sql.SparkSession
                                                                .builder
                                                                .config(conf=sparkConf)
                                                                .getOrCreate())
            return globals()['sparkSessionSingletonInstance']

        sqlContext = pyspark.sql.SQLContext(self.sc)
        sql_data = sqlContext.read.jdbc(url       =self.psql_config["url"],
                                        table     =self.psql_config["dbtable"],
                                        properties=self.psql_config["properties"])
        sql_data.createOrReplaceTempView("top10")
        sql_data.cache()

        SparkStreamerFromKafka.processStream(self)


        def process(time, rdd):
            global iPass
            try:
                iPass += 1
            except:
                iPass = 1

            print("========= Microbatch Number: {0} - {1} =========".format(iPass, str(time)))

            try:
                spark = getSparkSessionInstance(rdd.context.getConf())

                taxiRDD = sqlContext.read.json(rdd)

                taxiRDD.createOrReplaceTempView("taxiRDD")
                taxiDF = spark.sql("""SELECT * FROM taxiRDD""")
                taxiDF.createOrReplaceTempView("taxi")

                resDF = spark.sql("""SELECT
                                        taxi.vehicle_id AS id,
                                        taxi.datetime   AS datetime,
                                        taxi.longitude  AS lon,
                                        taxi.latitude   AS lat,
                                        top10.longitude AS top_lon,
                                        top10.latitude  AS top_lat
                                     FROM taxi LEFT JOIN top10
                                     ON  taxi.block_id_x = top10.block_id_x
                                     AND taxi.block_id_y = top10.block_id_y
                                     AND taxi.time_slot  = top10.time_slot""")

                resDF.show()

            except:
                pass

        self.dataStream.foreachRDD(process)
