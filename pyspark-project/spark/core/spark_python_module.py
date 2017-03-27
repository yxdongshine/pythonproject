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

# 导入时间模块
import time

if __name__ == '__main__':

    # 设置SPARK_HOME
    os.environ['SPARK_HOME'] = "D:/BaiduNetdiskDownload/spark-1.6.0-bin-2.5.0"

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName("Python Spark WordCount")\
        .setMaster("local[2]")

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()

