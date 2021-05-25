# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q1

from pyspark import SparkContext

def possible_pairs(line):
    user_id = line[0]
    friends = line[1].split(",")
    pairs = []
    for friend in friends:
        if friend != '' and friend != user_id:
            if int(user_id) < int(friend):
                pair = (user_id + "," + friend, set(friends))
            else:
                pair = (friend + "," + user_id, set(friends))
            pairs.append(pair)
    return pairs

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="Mutual Friends")
    lines = sc.textFile("./Input/soc-LiveJournal1Adj.txt")
    users = lines.map(
        lambda line: line.split("\t")).filter(
            lambda line: len(line) == 2).flatMap(possible_pairs)
    mutual_friends = users.reduceByKey(
        lambda x, y: x.intersection(y))
    mutual_friends.map(
        lambda x:"{0}\t{1}".format(x[0],len(x[1]))).sortBy(
            lambda x: x.split("\t")[0]).sortBy(
                lambda x: x.split("\t")[1]).saveAsTextFile("Output1")
