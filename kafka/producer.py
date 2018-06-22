import os, sys
sys.path.append("./helpers/")

import time
import json
import boto3
import lazyreader
import helpers
from kafka.producer import KafkaProducer


####################################################################

class Producer(object):
    """
    class that implements Kafka producers that ingest data from S3 bucket
    """

    def __init__(self, kafka_configfile, schema_file, s3_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of the S3 bucket and Kafka
        :type kafka_configfile: str
        :type s3_configfile: str
        """
        self.kafka_config = helpers.parse_config(kafka_configfile)
        self.kafka_config = helpers.replace_envvars_with_vals(self.kafka_config)

        self.schema = helpers.parse_config(schema_file)
        self.s3_config = helpers.parse_config(s3_configfile)

        self.producer = KafkaProducer(bootstrap_servers=self.kafka_config["BROKERS_IP"])


    def produce_msgs(self): # ,partition_key):
        """
        produces messages and sends them to topic
        """
        msg_cnt = 0

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_config["BUCKET"],
                            Key="{}/{}".format(self.s3_config["FOLDER"],
                                               self.s3_config["STREAMING_FILE"]))

        for line in lazyreader.lazyread(obj['Body'], delimiter='\n'):

            message_info = line.strip()
            msg = helpers.map_schema(message_info, self.schema)
            #print msg_cnt, message_info
            self.producer.send(self.kafka_config["TOPIC"],
                               json.dumps(msg))
            time.sleep(0.01)
            msg_cnt += 1



if __name__ == "__main__":

    if len(sys.argv) != 4:
        sys.stderr("Usage: producer.py <kafkaconfigfile> <schemafile> <s3configfile> \n")
        sys.exit(-1)

    kafka_configfile, schema_file, s3_configfile = sys.argv[1:4]

    prod = Producer(kafka_configfile, schema_file, s3_configfile)
    prod.produce_msgs() # ,partition_key)
