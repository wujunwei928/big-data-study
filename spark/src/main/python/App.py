"""SimpleApp"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
lines = sc.textFile("D:/big_data/spark-1.6.1-bin-hadoop2.6/README.md")
pythonLines = lines.filter(lambda line: "Python" in line)
print "----output----------------------"
print pythonLines.first()

