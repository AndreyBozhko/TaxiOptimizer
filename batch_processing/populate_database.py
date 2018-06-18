import sys
from transformers import TaxiBatchTransformer



if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write("populate_postgresql.py: Usage pyspark --jars $jars populate_postgresql.py $s3configfile $schema_configfile $postgresconfigfile \n")
        sys.exit(-1)

    s3_configfile, schema_configfile, psql_configfile = sys.argv[1:4]

    transformer = TaxiBatchTransformer(s3_configfile, schema_configfile, psql_configfile)
    transformer.run()
