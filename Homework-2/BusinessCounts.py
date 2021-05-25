# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q5

import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

sc = SparkContext(master="local", appName="Business Counts")
def helper(x):
	g = x[2][5:-1]
	g = g.split(',')
	h = []
	for i in g:
		if i != "":
			i = i.strip()
			if i[len(i)-1]==')':
				i = i[:len(i)-1]
			f = (i,1)
			h.append(f)
	return h

business = sc.textFile("./Input/business.csv").distinct().map(
    lambda x: x.split('::')).flatMap(helper).collect()
gg = sc.parallelize(business)
d = gg.reduceByKey(
    lambda x,y: x + y).map(
        lambda x: (x[1],x[0])).sortByKey(False).saveAsTextFile("Output5")
