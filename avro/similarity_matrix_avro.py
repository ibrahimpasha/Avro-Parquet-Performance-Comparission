from pyspark import SparkContext, SparkConf
from ast import literal_eval
import sys
from pyspark.sql import SparkSession, SQLContext

inputFiles = sys.argv[1]
outputFiles = sys.argv[2]
# inputFiles = '/bigd43/hw3/output_13.avro/*.avro'
# outputFiles = '/bigd43/hw3/output_17.avro'

conf = SparkConf().setAppName("Similarity Matrix Avro")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def findSimilarity(x):
	tpl = x
	emptyList = []
	for i in range(0, len(tpl[1])-1):
		l = tpl[1]
		for j in range(i+1, len(l)):
			#emptyList.append((tpl[0], ((l[i][0], l[j][0]), l[i][1]*l[j][1])))
			emptyList.append((tuple(sorted((l[i][0], l[j][0]))), l[i][1]*l[j][1]))
	return emptyList

inputAvro = sqlContext.read.format("com.databricks.spark.avro").load(inputFiles).rdd.map(tuple)
inverted_index = inputAvro\
	.map(findSimilarity)\
	.filter(lambda x: x!=[])\
	.map(lambda x: map(lambda tpl: tpl, x))\
	.flatMap(lambda x: x)\
	.map(lambda x: (x[0], x[1]))\
	.reduceByKey(lambda x, y: x+y, numPartitions=100)

df1 = sqlContext.createDataFrame(inverted_index, ['similar_docs', 'similarity'])
df1.write.format("com.databricks.spark.avro").save(outputFiles)
# inverted_index.saveAsTextFile(outputFiles)