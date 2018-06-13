import boto3


def get_files_to_read(producer_id, producer_count):

    with open('s3config') as fin:
        config = {row[0]:row[1] for row in map(lambda s: s.strip().split('='), fin.readlines())}
	bucketname, foldername, mask = config['BUCKET'], config['FOLDER'], config['FILEMASK']
   
    # get all contents of bucket folder
    keys = map(lambda obj: obj['Key'], boto3.client('s3').list_objects_v2(Bucket=bucketname, Prefix=foldername)['Contents'])
    
    # select only valid files
    keys = [k for k in keys if mask in k]

    # select files only for this producer
    keys = keys[producer_id-1::producer_count]  # producer_id starts from 1
    #print keys

    return [boto3.client('s3').get_object(Bucket=bucketname, Key=k) for k in keys]



#if __name__ == "__main__":
#    get_files_to_read(1, 1)
