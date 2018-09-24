from pyspark import SparkConf, SparkContext
from operator import add

def main():
    print("Hello Spark!!!")
    conf = SparkConf()
    conf.set("spark.app.name", "Mi script Python")

    # Iniciamos el SparkContext
    sc = SparkContext(conf=conf)
    sc.setLogLevel("FATAL")

    rdd = sc.parallelize(range(100000)).cache()

    rdd2 = rdd.map(lambda x: (x, 2*x))\
              .map(lambda (x,y): (x-100, y**2))\
              .reduceByKey(lambda x,y: x+y)\
              .values()

    r = rdd2.reduce(add)

    print("Resultado final = {0}".format(r))

    # Finalizamos el SparkContext
    sc.stop()

if __name__ == "__main__":
    main()