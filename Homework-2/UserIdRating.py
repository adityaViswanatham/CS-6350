# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q3

import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

sc = SparkContext(master="local", appName="UserId Rating")
business = sc.textFile("./Input/business.csv").map(
    lambda x: x.split('::')).map(
        lambda x: (x[0], tuple(x[1:])))
reviews = sc.textFile("./Input/review.csv").map(
    lambda x: x.split('::')).map(
        lambda x: (str(x[2]), (str(x[1]), float(x[3]))))
filtered_business = business.filter(
    lambda x: 'Stanford' in x[1][0])
result_data = filtered_business.join(reviews).map(
    lambda x: x[1][1]).distinct()
result = result_data.collect()
sc.parallelize(result).saveAsTextFile('Output3')