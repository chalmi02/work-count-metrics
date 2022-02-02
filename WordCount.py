import pyspark
from pyspark import SparkContext

if __name__=="__main__":

    configuration = pyspark.SparkConf().setMaster("local[*]").setAppName("Word Count Metrics")
    sc=pyspark.SparkContext(conf=configuration)
    lines = sc.textFile("sample.txt")
    words=lines.flatMap(lambda line : line.split(" ")) #On separe l'article en mots séparés

    wordCounts = words.countByValue()
    for word, count in wordCounts.items():
        print("{} : {}".format(word,count))