import boto3

class getmys3(object):

    def __init__(self):
	with open('s3config.txt') as fin:
	    self.bucket, self.folder, self.file = map(lambda s: s.strip().split('=')[1], fin.readlines())
	print self.file
    

    def get_object(self):
	return boto3.client('s3').get_object(Bucket=self.bucket, Key=self.folder+self.file)



if __name__ == "__main__":
    gg = getmys3()
