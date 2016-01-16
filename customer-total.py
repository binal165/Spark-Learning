from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

input = sc.textFile("/Users/bjhav1/Documents/SparkCourse/customer-orders.csv")
rdd = input.map(parseLine)
totalsByCustomer = rdd.reduceByKey(lambda x, y: x + y)
totalsByCustomerSorted = totalsByCustomer.map(lambda (x,y): (y,x)).sortByKey()
results = totalsByCustomerSorted.collect()
for result in results:
	print result