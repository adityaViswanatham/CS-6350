# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q4

import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

sc = SparkContext(master="local", appName="Top 10 Businesses")
business = sc.textFile("./Input/business.csv").map(
    lambda x: x.split('::')).map(
        lambda x: (x[0], (str(x[1]),str(x[2])))).distinct()
reviews = sc.textFile("./Input/review.csv").map(
    lambda x: x.split('::')).map(
        lambda x: (str(x[2]), (str(x[1]), float(x[3])))).distinct()

def find_average(x):
    rating = []
    column = 0
    for i in x:
        for j in i:
            column = column + 1
            if column % 2 == 0:
                rating.append(j)

    return sum(rating) / len(rating)

result = reviews.groupByKey().mapValues(find_average).collect()[:]
rating = sc.parallelize(result)
top_ten = rating.join(business).map(
    lambda x: (x[1][0], (x[1][1][0], x[1][1][1], x[0]))).sortByKey(False)
top_results = top_ten.top(10)
final_result = sc.parallelize(top_results)
final_result = final_result.map(
    lambda x:"{0}\t {1}\t\t {2}\t\t\t {3}".format(x[1][2],x[1][0],x[1][1],x[0]))
final_result.saveAsTextFile('Output4')