peg config | grep 'access key' | sed s/'.*key: '/'AWS_ACCESS_KEY='/g
peg config | grep 'secret key' | sed s/'.*key: '/'AWS_SECRET_KEY='/g
