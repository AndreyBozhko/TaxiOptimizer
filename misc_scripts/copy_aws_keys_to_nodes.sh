TMP_FILE=awskeys.txt
DEST_FILE=.bashrc

NODE=ubuntu@ec2-34-237-30-114.compute-1.amazonaws.com

printf "\n" > $TMP_FILE
peg config | grep 'access key' | sed s/'.*key: '/'AWS_ACCESS_KEY='/g >> $TMP_FILE
peg config | grep 'secret key' | sed s/'.*key: '/'AWS_SECRET_KEY='/g >> $TMP_FILE

scp $TMP_FILE $NODE:
ssh $NODE 'cat '"$TMP_FILE"' >> '"$DEST_FILE"
ssh $NODE 'rm '"$TMP_FILE"
