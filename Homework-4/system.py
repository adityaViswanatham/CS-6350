# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350 Homework-4

import math
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="recc system")
    ratings = sc.textFile("./Input/ratings.dat").map(lambda x: x.split("::"))
    ratings = ratings.map(
        lambda x: (x[0], x[1], x[2]))
    train, test = ratings.randomSplit([6, 4], seed=0)
    prediction_test = test.map(
        lambda x: (x[0], x[1]))

    seed = 10
    iterations = 20
    ranks = [4, 8, 10, 12, 16]
    errors = [0, 0, 0, 0, 0]
    err = 0
    reg = 0.1
    min_error = float('inf')

    for rank in ranks:
        model = ALS.train(train, rank, seed=seed, iterations=iterations, lambda_=reg , nonnegative=False, blocks=-1)
        predictions = model.predictAll(prediction_test).map(
            lambda r: (((r[0], r[1]), r[2])))
        rates_preds = test.map(
            lambda x: (((int(x[0]), int(x[1])), float(x[2])))).join(predictions)
        error = math.sqrt(rates_preds.map(
            lambda x: (x[1][0] - x[1][1]) ** 2).mean())
        errors[err] = error
        err += 1

        if (error < min_error):
            min_error = error
    print("Best MSE", min_error)