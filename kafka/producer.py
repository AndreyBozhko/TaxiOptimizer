import sys
from getmys3 import getmys3
#import boto3
from kafka.producer import KafkaProducer


class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)
        #self.load_s3('andrey-bozhko-bucket-insight', 'nyc_taxi_raw_data/small/trip_data_small_test.csv')
	gg = getmys3()
	self.obj = gg.get_object()
	self.topic = 'my-topic'


    #def load_s3(self, bucket, key):
    #    s3 = boto3.client('s3')
    #    self.obj = s3.get_object(Bucket=bucket, Key=key)


    def stream_from_s3(self):
        return self.obj['Body']._raw_stream.readline().strip()


    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            
            try:
                message_info = self.stream_from_s3()
           
		if message_info == "":
                    break		
 
                print msg_cnt, message_info
                self.producer.send(self.topic, message_info)
                msg_cnt += 1

            except:
                break



if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
