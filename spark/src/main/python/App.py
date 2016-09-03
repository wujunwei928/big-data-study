"""SimpleApp"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#
# lines = sc.textFile("/data/spark/README.md")
# pythonLines = lines.filter(lambda line: "Python" in line)
# print "----output----------------------"
# print pythonLines.first()


# sc is an existing SparkContext.
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH '/data/spark/examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results = sqlContext.sql("FROM src SELECT key, value").collect()

