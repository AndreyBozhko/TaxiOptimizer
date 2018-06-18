import sys
import boto3
import operator
import lazyreader
import helpers
from kafka.producer import KafkaProducer


####################################################################

class Producer(object):
    """
    class that implements Kafka producers that ingest data from S3 bucket
    """

    def __init__(self, topic, addr, pid, cnt, s3_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of the S3 bucket, topic, broker ip and producer id and count
        :type topic: str
        :type addr: str
        :type pid: int
        :type cnt: int
        :type s3_configfile: str
        """
        self.producer = KafkaProducer(bootstrap_servers=addr)
        self.s3_config = helpers.parse_config(s3_configfile)
        self.objs = get_files_to_read(pid, cnt)
        self.topic = topic


    def clean_stream_data(self, msg): # schema
        """
        cleans the message msg, leaving only the fields given by schema
        :type msg: str
        :type schema: dict
        :rtype : dict ??? json
        """
        try:
            msg = ",".join(operator.itemgetter(0,1,2,3)(msg.split('\t')))
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
            self.producer.send(self.topic, self.clean_stream_data(message_info))
            msg_cnt += 1



if __name__ == "__main__":

    args = sys.argv
    if len(args) != 6:
        sys.stderr("producer.py: Usage producer.py $topic $ip_addr $producer_id $producer_count $s3_configfile \n")
        sys.exit(-1)

    topic, ip_addr = map(str, args[1:3])
    producer_id, producer_count = map(int, args[3:5])
    s3_configfile = args[5]

    prod = Producer(topic, ip_addr, producer_id, producer_count, s3_configfile)
    prod.produce_msgs() # ,partition_key)
