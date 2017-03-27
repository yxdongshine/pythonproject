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

    """
        TaoBao:
            page_views
        Data Structure:
            track_time          	string
            url                 	string
            session_id          	string
            referer             	string
            ip                  	string
            end_user_id         	string
            city_id             	string
    """
    # transform function
    def map_func(line):
        # split line
        arr = line.split("\t")
        # return Row
        return Row(track_time=arr[0], url=arr[1] ,session_id=arr[2], referer=arr[3],
                   ip=arr[4], end_user_id=arr[5], city_id=arr[6])

    # read data and transform RDD[Row]
    page_views_rdd = sc\
        .textFile("/user/hive/warehouse/db_track.db/yhd_log/")\
        .map(map_func)
    # print page_views_rdd.first()

    # Create DataFrame
    page_views_df = sqlContext.createDataFrame(page_views_rdd)
    # Register Temp Table
    page_views_df.registerTempTable("tmp_page_views")

    # SQL Analyzer
    count_result = sqlContext.sql("""
        SELECT COUNT(*) As total FROM tmp_page_views
    """).map(lambda output: "Count: " + str(output.total))
    # for result in count_result.collect():
    #     print result

    session_id_count_result = sqlContext.sql("""
        SELECT
            session_id, COUNT(1) AS total
        FROM
            tmp_page_views
        GROUP BY
            session_id
        ORDER By
            total DESC
        LIMIT
            10
    """).map(lambda output: output.session_id + " \t" + str(output.total))
    # for result in session_id_count_result.collect():
    #     print result

    # DSL Analyzer
    count_dsl_result = page_views_df\
        .groupBy("session_id")\
        .count()\
        .sort("count", ascending=False)\
        .limit(10)\
        .collect()
    for result in count_dsl_result:
        print str(result['session_id']) + "\t" + str(result['count'])

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()

