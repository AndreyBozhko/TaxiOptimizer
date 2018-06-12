import sys
import operator
from read_from_s3 import get_files_to_read
from kafka.producer import KafkaProducer


class Producer(object):

    def __init__(self, topic, addr, pid, cnt):
        self.producer = KafkaProducer(bootstrap_servers=addr)
	self.objs = get_files_to_read(pid, cnt)
	self.topic = topic


    def enforce_schema(self, msg):
	return "".join(operator.itemgetter(5,7,10,11)(msg.split(',')))


    def produce_msgs(self): # ,partition_key):
        msg_cnt = 0
	
	for obj in self.objs:
            
	    while True:
            
                message_info = obj['Body']._raw_stream.readline().strip()
           
		if message_info == "":
                    break		
 
                #print msg_cnt, message_info
                self.producer.send(self.topic, self.enforce_schema(message_info))
                msg_cnt += 1



if __name__ == "__main__":
    args = sys.argv
    topic, ip_addr = map(str, args[1:3])
    producer_id, producer_count = map(int, args[3:5])

    prod = Producer(topic, ip_addr, producer_id, producer_count)
    prod.produce_msgs() # ,partition_key)
