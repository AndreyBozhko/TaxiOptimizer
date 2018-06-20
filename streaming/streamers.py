import sys
sys.path.append("./helpers/")

import pyspark
import helpers
from pyspark.streaming.kafka import KafkaUtils


####################################################################

class SparkStreamerFromKafka:

    def __init__(self, kafka_configfile, schema_configfile):
        """
        constructor
        """
        batch_interval = 1   # 1 second
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
                                    .map(helpers.add_block_fields)
                                    .map(helpers.add_time_slot_field)
                                    .filter(lambda x: x is not None))


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
        sql_data = self.load_batch_data()
        sql_data.broadcast()
        #print sql_data.select("subblock").where("block_id=5558").where("time_slot=100")

        SparkStreamerFromKafka.processStream(self)

        results = (self.dataStream
                            .map(lambda taxi: helpers.get_top_spots(taxi, sql_data)))
                            #                   "time_slot":helpers.determine_time_slot(line["time_received"])})
                            #.map(helpers.get_top_spots))

        result.pprint()
