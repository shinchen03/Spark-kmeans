import sys
from  math import sqrt, radians, sin, cos, asin
from pyspark import SparkContext


class kmeansAPI:
    def __init__(self, similarityMeasure):
        self.similarityMeasure = similarityMeasure

    def closestPoint(self, x, y, centers):
        """
        Given a (latitude/longitude) point and an array of current center points,
        returns the index in the array of the center closest to the given point
        """
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
        """
        Given two points, return a point which is the sum of the two points
        """
        return (point1[0] + point2[0], point1[1] + point2[1])

    def EuclideanDistance(self, point1, point2):
        """
        Given two points, return the Euclidean distance between two points
        """
        return sqrt((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2)

    def GreatCircleDistance(self, point1, point2):
        """
        Given two points, return the Great circle distance between two points
        """
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

    def addAllPoints(self, centers):
        point = (0, 0)
        for center in centers:
            point = self.addPoint(point, center)
        return point

    def meanChange(self, center, newCenter):
        """
        Calculate the mean change between the old centers and new centers
        """

        dist = 0
        for i in range(len(center)):
            if self.similarityMeasure == "EuclideanDistance":
                dist += self.EuclideanDistance(center[i], newCenter[i])
            else:
                dist += self.GreatCircleDistance(center[i], newCenter[i])
        return dist / len(center)
    
    def measure_S(self, clusters, centers):
        """
        Calculate the distance between each point in the clusters
        """
        sim = 0
        count = 0
        for i in range(len(clusters)):
            for j in range(len(clusters[i])):
                sim += self.EuclideanDistance(clusters[i][j], centers[i])
                    count += 1
        return sim/count

    def measure_D(self, centers):
        """
        Calculate the distance between each point in one cluster and the centroid of the other cluster
        """
        sim = 0
        count = 0
        for i in range(len(centers)):
            for j in range(i,len(centers)):
                sim += self.EuclideanDistance(centers[i], centers[j])
                    count += 1
        return sim/count

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: k-means.py <file> k similarityMeasure"
        exit(-1)
    sc = SparkContext(appName="k-means")
    # persist the resulting rdd
    lines = sc.textFile(sys.argv[1]).cache()
    k = sys.argv[2]
    similarityMeasure = sys.argv[3]
    kmeans = kmeansAPI(similarityMeasure)
    centers = []
    # Extract the latitude and longitude and convert it to float
    data = lines.map(lambda x: x.split(',')).map(lambda x: (float(x[0]),float(x[1])))
    # randomly take k points from the input as initial centers
    centers = data.takeSample(False, int(k))
    newCenters = [(0,0) for _ in range(int(k))]
    i = 0
    while kmeans.meanChange(centers, newCenters) > 0.1:
        centers = newCenters if i > 0 else centers
        # assign each point to the closest cluster
        centerIndex = data.map(lambda point: (centers[kmeans.closestPoint(point[0],point[1], centers)], point))
        # count the number of points in each cluster
        numPoints = centerIndex.countByKey()
        clusters = centerIndex.reduceByKey(lambda point1, point2: kmeans.addPoint(point1, point2))
        # get the new center as the centroid of the new clusters
        newCenters = clusters.map(lambda x: (x[1][0]/numPoints[x[0]],x[1][1]/numPoints[x[0]])).collect()
        i += 1
     # get a list of the final clusters
     clusters = data.map(lambda point: (newCenters[kmeans.closestPoint(point[0],point[1], newCenters)], point)) \
			   .groupByKey() \
           		   .map(lambda x: list(x[1]))
     S = kmeans.measure_S(clusters.collect(),newCenters)
     D = kmeans.measure_D(newCenters)
     print(newCenters)
     clusters.saveAsTextFile("syntheticClusters2")
     
