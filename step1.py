import sys
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, "Usage: step1 <file>"
		exit(-1)
	sc = SparkContext(appName="DataPreparation")
	lines = sc.textFile(sys.argv[1])
	pre_process = lines.map(lambda x: x.encode("ascii", "ignore").split(',')) \
					   .filter(lambda x: len(x) == 14) \
					   .map(lambda fields: (fields[12],fields[13],fields[0],fields[1],fields[2]))
	extracted_data = pre_process.filter(lambda field: field[0] != '0' and field[1] != '0') \
				    .map(lambda x: x[0]+","+x[1]+","+x[2]+","+x[3].split(" ")[0]+","+x[3].split(" ")[1]+","+x[4])
	extracted_data.saveAsTextFile("preprocessed_data4.txt")
