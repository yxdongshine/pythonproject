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
from pyspark import SQLContext, Row

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
    sqlContext = SQLContext(sparkContext=sc)

    # =============================  First =====================
    # definition function
    def squared_func(number):
        return number * number
    # register function
    # def register(self, name, f, returnType=StringType())
    sqlContext.udf.register("squaredWithPython", squared_func)

    # =============================  Second =====================
    # import
    from pyspark.sql.types import LongType

    def squared_func_typed(number):
        return number * number
    sqlContext.udf.register("squaredWithPythonTyped", squared_func_typed, returnType=LongType())

    # =============================  Third =====================
    sqlContext.udf.register("squaredWithPythonLambda", lambda number: number * number)

    """
        Call the UDF in Spark SQL
    """
    # SCALA: Range(1, 20)
    sqlContext.range(1, 20).registerTempTable("test")
    # SQL
    squared_result = sqlContext\
        .sql("SELECT id,  squaredWithPython(id) as id_squared FROM test")\
        .collect()
    for result in squared_result:
        print str(result['id']) + ", " + str(result['id_squared'])

    # ================================================================
    """
        Use UDF with DataFrame
    """
    from pyspark.sql.functions import udf
    squared_udf = udf(squared_func_typed, LongType())
    df = sqlContext.table("test")
    for result in df.select("id", squared_udf("id").alias("id_squared")).collect():
        print str(result['id']) + ", " + str(result['id_squared'])

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()

