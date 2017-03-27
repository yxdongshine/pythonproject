#!/usr/bin/python
# encoding:utf-8

"""
@author: xuanyu
@contact: xuanyu2015@126.com
@file: spark_word_count.py
@time: 2017/3/25 10:17
"""

# 导入py spark模块
from pyspark import SparkConf, SparkContext

# 导入系统设置模块
import os
import sys

# 导入时间模块
import time

if __name__ == '__main__':

    # 设置SPARK_HOME, 就是客户端
    os.environ['SPARK_HOME'] = "D:/BaiduNetdiskDownload/spark-1.6.0-bin-2.5.0"

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName("Python Spark WordCount") \
        .setMaster("local[2]")
    # .setMaster(sys.argv[1])

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)

    """
        创建RDD：
            方式一：从本地集合进行并行化创建
            方式二：从外部存储系统读取数据
    """
    # 第一方式：从集合中并行化创建RDD
    # List
    # data = ["hadoop spark", "spark hive spark sql", "spark hadoop sql sql spark"]
    # Create RDD
    # rdd = sc.parallelize(data)

    # 第二种方式：从HDFS文件中读取数据
    # 此种方式要注意：需要将HDFS CLIENT配置文件放入$SPARK_HOME/conf目录下
    rdd = sc.textFile("/sparksql/csvdata/")

    # print rdd.count()

    # =======================================================================
    # 词频统计
    word_count_rdd = rdd\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)
    # output
    # for x in word_count_rdd.collect():
    #     # print x
    #     print x[0] + ", " + str(x[1])

    # =======================================================================
    # 获取词频出现最多的单词
    # sortByKey
    sort_rdd = word_count_rdd\
        .map(lambda (word, count): (count, word))\
        .sortByKey(ascending=False)\
        .take(3)
    # output
    # for (sort_count, sort_word) in sort_rdd:
    #     # print x
    #     # print str(x[0]) + ", " + x[1]
    #     print ("%s: %i" % (sort_word, sort_count))

    # =======================================================================
    # 获取词频出现最多的单词
    # top
    top_rdd = word_count_rdd\
        .top(3, key=lambda (word, count): count)
    for tup in top_rdd:
        print tup

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(10000)

    # SparkContext Stop
    sc.stop()

