import os, sys
sys.path.append("../helpers/")

import pyspark
import helpers
#import pyspark.streaming
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
        self.schema = helpers.parse_config(schema_configfile)


    def produceStream(self):
        """
        initializes stream
        """
        self.dataStream = KafkaUtils.createDirectStream(self.scc,
                                                        [self.kafka_config["TOPIC"]],
                                                        #{"metadata.broker.list": self.kafka_config["BROKERS"]})
                                                         {"bootstrap.servers": self.kafka_config["BROKERS_IP"]})


    def run(self):
        """
        executes init and starts streaming
        """
        self.produceStream()
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


    def produceStream(self):
        """
        initializes stream
        """
        SparkStreamerFromKafka.produceStream(self)
        self.dataStream.pprint()
