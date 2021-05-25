# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW2-Q7

from pyspark import SparkContext

counter = 1

def allocate_lines(line):
    words = line.split(",")
    temp = []
    global counter
    for word in words:
        other = []
        other.append(counter)
        pair = (word, other)
        temp.append(pair)
    counter += 1
    return temp

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="Inverted Index")
    text_file = sc.textFile("./Input/userdata.txt")
    lines = text_file.flatMap(allocate_lines).reduceByKey(lambda x, y: x + y)
    lines.map(lambda x:"{0}\t{1}".format(x[0], x[1])).saveAsTextFile("Output7")