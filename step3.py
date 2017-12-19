import sys
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, "Usage: step1 <file>"
		exit(-1)
	sc = SparkContext(appName="DataPreparation")
	lines = sc.textFile(sys.argv[1])
	pre_process = lines.map(lambda x: x.encode("ascii", "ignore").split(" ")).filter(lambda x: 24.7433195 < float(x[0]) < 49.3457868 and -124.7844079 < float(x[1]) < -66.9513812)
	data = pre_process.map(lambda x: x[0]+","+x[1])
	data.saveAsTextFile("hdfs:/user/training/preprocessed_data_step3.txt")


