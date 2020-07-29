import sys
import pyspark

from pyspark.sql import SparkSession
from pyspark.context import SparkContext

sc = SparkSession.builder.appName("First Task")
sc = sc.getOrCreate()
data_csv = sc.read.csv('airports.csv',header=True)

#data_csv.show(10)

cpu_count = int(sys.argv[1])
output = str(sys.argv[2])



#paritioned_data = data_csv.repartition(cpu_count)
grouped = data_csv.groupBy("COUNTRY")
grouped = grouped.count()
paritioned = grouped.repartition(cpu_count)
#grouped.show(10)
#grouped.repartition(1).save(output,'csv','overwrite')
paritioned.repartition(1).write.csv(output,mode="overwrite")
sc.stop()
