%pyspark
from  math import sqrt, radians, sin, cos, asin


class kmeansAPI:
    def __init__(self, similarityMeasure):
        self.similarityMeasure = similarityMeasure

    def closestPoint(self, x, y, centers):
        smallestDist = float('inf')
        closestIndex = 0
        if self.similarityMeasure == 'EuclideanDistance':
            for index, center in enumerate(centers):
                if smallestDist > self.EuclideanDistance((x, y), center):
                    cloestIndex = index
                    smallestDist = self.EuclideanDistance((x, y), center)
        else:
            for index,center in enumerate(centers):
                if smallestDist > self.GreatCircleDistance((x, y), center):
                    cloestIndex = index
                    smallestDist = self.GreatCircleDistance((x, y), center)
        return cloestIndex

    def addPoint(self, point1, point2):
        return (point1[0] + point2[0], point1[1] + point2[1])

    def EuclideanDistance(self, point1, point2):
        return sqrt((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2)

    def GreatCircleDistance(self, point1, point2):
        # convert decimal degrees to radians
        lat1, lon1 = point1
        lat2, lon2 = point2
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        # Radius of earth in kilometers is 6371
        km = 6371 * c
        return km

    def meanChange(self, center, newCenter):
        dist = 0
        for i in range(len(center)):
            if self.similarityMeasure == "EuclideanDistance":
                dist += self.EuclideanDistance(center[i], newCenter[i])
            else:
                dist += self.GreatCircleDistance(center[i], newCenter[i])
        return dist / len(center)

    def measure_S(self, clusters, centers):
		sim = 0
		count = 0
		for i in range(len(clusters)):
			for j in range(len(clusters[i])):
				sim += self.EuclideanDistance(clusters[i][j], centers[i])
				count += 1
		return sim/count

    def measure_D(self, centers):
		sim = 0
		count = 0
		for i in range(len(centers)):
			for j in range(i,len(centers)):
				sim += self.EuclideanDistance(centers[i], centers[j])
				count += 1
		return sim/count



k = 6
out = ""
similarityMeasure = "EuclideanDistance"
lines = sc.textFile('s3://lizhen.xiang-emr/input/latlongin').cache()
while k < 21:
    print("K = " + str(k))
    kmeans = kmeansAPI(similarityMeasure)
    centers = []
    data = lines.map(lambda x: x.split(" ")).map(lambda x: (float(x[0]),float(x[1])))
    centers = data.takeSample(False, int(k))
    newCenters = [(0,0) for _ in range(int(k))]
    i = 0
    while kmeans.meanChange(centers, newCenters) > 0.1:
        centers = newCenters if i > 0 else centers
        centerIndex = data.map(lambda point: (centers[kmeans.closestPoint(point[0],point[1], centers)], point))
        numPoints = centerIndex.countByKey()
        clusters = centerIndex.reduceByKey(lambda point1, point2: kmeans.addPoint(point1, point2))
        newCenters = clusters.map(lambda x: (x[1][0]/numPoints[x[0]],x[1][1]/numPoints[x[0]])).collect()
        i += 1
    clusters = data.map(lambda point: (newCenters[kmeans.closestPoint(point[0],point[1], centers)], point)) \
    			   .groupByKey() \
    			   .map(lambda x: list(x[1]))
    S = kmeans.measure_S(clusters.collect(),newCenters)
    D = kmeans.measure_D(newCenters)
    temp = "k = " + str(k) + " S = " +str(S) + " D = " +str(D)+" S/D = "+str(S/D)+ "\n"
    print(temp)
    out =  "\n" + out + temp
    if k == 20:
        #out.saveAsTextFile('s3://lizhen.xiang-emr/output/kmeans-test1')
        print(out)
    k = k+1

#out.saveAsTextFile('s3://lizhen.xiang-emr/output/kmeans-test')
print(out)
