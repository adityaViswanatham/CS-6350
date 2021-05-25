# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q2

from pyspark import SparkContext, SparkConf

def common_friends(line):
    user = line[0].strip()
    friends = line[1]
    if user != '':
        temp = []
        for friend in friends:
            friend = friend.strip()
            if friend != '':
                if float(friend) < float(user):
                    user_friend = (friend + "," + user, set(friends))
                else:
                    user_friend = (user + "," + friend, set(friends))
                temp.append(user_friend)
        return temp

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="Mutual Friends Top")
    users = sc.textFile("./Input/soc-LiveJournal1Adj.txt").map(
        lambda x: x.split("\t")).filter(
            lambda x: len(x) == 2).map(
                lambda x: [x[0], x[1].split(",")])
    users_mutual_friends = users.flatMap(common_friends)
    mutual_friends = users_mutual_friends.reduceByKey(
        lambda x,y: len(x.intersection(y)))
    final = mutual_friends.map(
        lambda x: (x[1],x[0])).sortByKey(False)
    final_result = final.collect()
    max_value = final_result[0][0]
    final.filter(
        lambda x: x[0] == max_value).map(
            lambda x:"{0}\t{1}".format(x[1],x[0])).saveAsTextFile("Output2")