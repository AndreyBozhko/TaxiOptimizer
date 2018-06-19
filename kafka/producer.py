import os, sys
sys.path.append("../helpers/")

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
        self.schema = helpers.parse_config(schema_file)
        self.s3_config = helpers.parse_config(s3_configfile)

        self.producer = KafkaProducer(bootstrap_servers=self.kafka_config["BROKERS_IP"])


    def clean_stream_data(self, msg):
        """
        cleans the message msg, leaving only the fields given by schema
        :type msg: str
        :rtype : json
        """
        try:
            msg = msg.split('\t')
            msg = {key:msg[self.schema[key]] for key in self.schema.keys()}
            msg = json.dumps(msg)
        except:
            msg = ""
        return msg


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

            #print msg_cnt, message_info
            self.producer.send(self.kafka_config["TOPIC"],
                               self.clean_stream_data(message_info))
            msg_cnt += 1



if __name__ == "__main__":

    args = sys.argv
    if len(args) != 4:
        sys.stderr("Usage: producer.py <kafkaconfigfile> <schemafile> <s3configfile> \n")
        sys.exit(-1)

    kafka_configfile, schema_file, s3_configfile = args[1:4]

    prod = Producer(kafka_configfile, schema_file, s3_configfile)
    prod.produce_msgs() # ,partition_key)
