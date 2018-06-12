CLUSTER=$1

peg describe $CLUSTER | grep 'ec2*' | sed s/'Public DNS: '/'ubuntu@'/g
