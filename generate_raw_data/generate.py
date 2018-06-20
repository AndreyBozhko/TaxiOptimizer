import sys
sys.path.append("../helpers/")

import numpy
import helpers
import pyspark
import pyspark.sql
from datetime import datetime, timedelta



def transform(line, i):
    """
    generates data by adding some random noise to historical data
    !!! takes 6 min to generate month of data (on 11 m4.large instances) !!!
    :type line: str
    :type i: int
    :rtype : str
    """
    #i1, i3, i5, i7 = 5, 7, 10, 12
    i1, i3, i5, i7 = 1, 3, 5, 7
    s = line.strip().split(",")


    try:
        # change date and time
        dt1, dt2 = [datetime.strptime(s[i], "%Y-%m-%d %H:%M:%S") for i in [i1, i1+1]]
        delta = dt2-dt1
        dt1 = datetime(dt1.year-7*(i+1),
                       dt1.month,
                       dt1.day,
                       dt1.hour,
                       dt1.minute,
                       dt1.second)
        dt1 += timedelta(seconds=numpy.random.randint(-250, 250))
        dt2 = dt1 + delta
        s[i1], s[i1+1] = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in [dt1, dt2]]

        # change passengers
        s[i3] = str(int(s[i3])+1) if numpy.random.random() > 0.75 else s[i3]

        #change coordinates
        lon, lat = map(float, s[i5:i7])
        lon += numpy.random.randint(-10, 10)/float(10**5)
        lat += numpy.random.randint(-10, 10)/float(10**5)
        s[i5:i7] = map(str, [lon, lat])
    except:
        return

    return s[:]



if __name__ == '__main__':

    if len(sys.argv) != 5:
        sys.stderr.write("Usage: spark-submit generate.py <year> <month> <bucket> <folder> \n")
        sys.exit(-1)

    year, month, bucket, folder = sys.argv[1:5]
    prefix = "s3a://{}/{}/".format(bucket, folder)
    filename = "trip_data_{}_{}{}.csv"

    sc = pyspark.SparkContext.getOrCreate()
    sqlcontext = pyspark.sql.SQLContext(sc)
    data = sc.textFile(prefix+filename.format(year, month, ""))

    for i in range(1,3):
        data2 = sqlcontext.createDataFrame(data.map(lambda line: transform(line, i)).filter(lambda x: x is not None))
        data2.repartition(1).write.csv(prefix+filename.format(int(year)-7*i, month, "_s"))
