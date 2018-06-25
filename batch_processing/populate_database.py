import sys
import pyspark
from batch_transformers import TaxiBatchTransformer



### main method that executes batch job ###

if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write("Usage: spark-submit --jars <jars> populate_database.py <s3configfile> <schemaconfigfile> <postgresconfigfile> \n")
        sys.exit(-1)

    s3_configfile, schema_configfile, psql_configfile = sys.argv[1:4]

    transformer = TaxiBatchTransformer(s3_configfile, schema_configfile, psql_configfile)
    transformer.run()
