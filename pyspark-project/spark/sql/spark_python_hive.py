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
from pyspark import HiveContext

# 导入系统设置模块
import os

# 导入时间模块
import time

if __name__ == '__main__':

    # 设置SPARK_HOME
    os.environ['SPARK_HOME'] = "D:/BaiduNetdiskDownload/spark-1.6.0-bin-2.5.0"

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName("Python Spark SQL")\
        .setMaster("local[2]")
    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # Create SQLContext
    sqlContext = HiveContext(sparkContext=sc)

    # Hive Table
    emp_df = sqlContext.read.table("metastore.emp_external_part")
    dept_df = sqlContext.read.table("metastore.dept_external")

    # JOIN
    join_result = emp_df\
        .join(dept_df, on='deptno')\
        .select('empno', 'ename', 'sal', 'dname')\
        .take(14)
    for result in join_result:
        # print
        print str(result['empno']) + "\t" + str(result['sal']) + "\t" + str(result['dname'])

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()

