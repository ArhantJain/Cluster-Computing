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
output = sys.argv[2]


# data_csv.show(10) # 1st time
paritioned = data_csv.repartition(cpu_count)
grouped_data = paritioned.groupBy("COUNTRY")#.show(false)
grouped_data = grouped_data.count().sort(desc("count"))
#grouped_data = grouped_data.agg(max("count"))
output1 = grouped_data.select("COUNTRY").first()
#print("**************")
ans =  output1[0]
print("Country Having Highest Number of Airports is ")
print(ans)
#print("**************")

# paritioned.show(10)  # 3rd time

#grouped.repartition(1).save(output,'csv','overwrite')
f = open(output,"w+")
f.write(ans)
f.close()
#paritioned.write.csv(output,mode="overwrite")
sc.stop()

# data_csv.select(data_csv.groupBy("COUNTRY").count().alias("Total")).show(10)