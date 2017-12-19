import sys
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, "Usage: step1 <file>"
		exit(-1)
	sc = SparkContext(appName="DataPreparation")
	lines = sc.textFile(sys.argv[1])
	pre_process = lines.filter(lambda x: len(x) != 0).map(lambda x: x.encode("ascii", "ignore").split())
	data = pre_process.map(lambda x: x[0] + ',' + x[1])
	data.saveAsTextFile("preprocessed_data_step2")

