import os
import sys
#import json
#import psycopg2
import helpers
import pyspark
#import pyspark.sql
from pyspark.sql.types import ArrayType, IntegerType


class BatchTransformer(object):
    """
    class that reads data from S3 bucket, processes it with Spark
    and saves the results into PostgreSQL database
    """

    def __init__(self, s3_configfile, schema_configfile, psql_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of the S3 bucket, ??? and PostgreSQL table
        :type s3_configfile: str
        :type schema_configfile: str
        :type psql_configfile: str
        """
        self.psql_config = self.get_psql_config(psql_configfile)
        self.s3_config = self.get_s3_config(s3_configfile)
        self.sc = pyspark.SparkContext.getOrCreate()


    def get_s3_config(self, s3_configfile):
        """
        returns configurations of the S3 bucket
        :type s3_configfile: str
        :rtype : dict
        """
        return helpers.parse_config(s3_configfile)


    def get_psql_config(self, psql_configfile):
        """
        returns configurations of the PostgreSQL table
        :type psql_configfile: str
        :rtype : dict
        """
        config = helpers.parse_config(psql_configfile)
        config["url"] = "{}{}:{}/".format(config["url"], config["host"], config["port"])
        if "password" not in config["properties"].keys():
            passfile = config["passfile"].replace("~", os.getenv("HOME"))
            with open(passfile) as fin:
                config["properties"]["password"] = fin.readline().strip().split(":")[-1]
        return config


    def read_from_s3(self):
        """
        reads files from s3 bucket defined by s3_configfile and creates Spark RDD
        :type s3_configfile: str
        """
        filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"],
                                           self.s3_config["FOLDER"],
                                           self.s3_config["RAW_DATA_FILE"])
        self.data = self.sc.textFile(filename)


    def save_to_postgresql(self):
        """
        saves result of batch transformation to PostgreSQL database
        :type data: Spark RDD
        """
        sqlContext = pyspark.sql.SQLContext(self.sc)

        sql_data = sqlContext.createDataFrame(self.data) # need to use Row
        sql_data.write.jdbc(       url=self.psql_config["url"],
                                 table=self.psql_config["dbtable"],
                                  mode=self.psql_config["mode"],
                            properties=self.psql_config["properties"])


    def spark_transform(self):
        """
        transforms Spark RDD with raw data into RDD with cleaned data
        """
        self.data = (self.data.map(helpers.clean_data)
                         .filter(lambda x: x is not None))


    def run(self):
        """
        executes the read from s3, transform by spark and write to postgresql sequence
        """
        self.read_from_s3()
        self.spark_transform()
        self.save_to_postgresql()



class TaxiBatchTransformer(BatchTransformer):
    """
    class that calculates the top-10 pickup spots from historical data
    """

    def spark_transform(self):
        """
        transforms Spark RDD with raw data into the RDD that contains
        top-10 pickup spots for each block and time slot
        """
        super(TaxiBatchTransformer, self).spark_transform()
        udf = lambda a: "::".join(map(lambda x: ",".join(map(str, x)), a)) # temporary solution
        self.data = (self.data
                        .map(lambda x: ((x["block_id"], x["time_slot"], x["sub_block_id"]), x["passengers"]))
                        .reduceByKey(lambda x,y : x+y)
                        .map(lambda x: ((x[0][0], x[0][1]), [x[0][2], x[1]]))
                        .groupByKey()
                        .map(lambda x: {"block_id": x[0][0], "time_slot": x[0][1], "subblock_psgcnt": udf(sorted(x[1], key=lambda z: -z[1])[:10])}))



if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write("populate_postgresql.py: Usage pyspark --jars $jars populate_postgresql.py $s3configfile $schema_configfile $postgresconfigfile \n")
        sys.exit(-1)

    s3_configfile, schema_configfile, psql_configfile = sys.argv[1:4]

    transformer = TaxiBatchTransformer(s3_configfile, schema_configfile, psql_configfile)
    transformer.run()
