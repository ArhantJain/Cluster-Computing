import sys
import pyspark

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkSession.builder.appName("First Task")
sc = sc.getOrCreate()
data_csv = sc.read.csv('airports.csv',header=True)

#data_csv.show(10)

cpu_count = int(sys.argv[1])
output = str(sys.argv[2])


cond1 = (data_csv.LATITUDE <= int(90)) & (data_csv.LATITUDE >= int(10))
cond2 = (data_csv.LONGITUDE <= int(-10)) & (data_csv.LONGITUDE >= int(-90))
#paritioned_data = data_csv.repartition(cpu_count)
grouped = data_csv.filter(cond1 & cond2)

#grouped = data_csv.select("LATITUDE","LONGITUDE",when(cond1&cond2,1).otherwise(0)).show(10)

#grouped = data_csv.select("ID","TYPE","NAME","LATITUDE","LONGITUDE","COUNTRY",
#	"REGION",when(cond2,1))


#grouped.show()
paritioned = grouped.repartition(cpu_count)
#grouped.show(10)
#grouped.repartition(1).save(output,'csv','overwrite')
paritioned.repartition(1).write.csv(output,mode="overwrite")
sc.stop()