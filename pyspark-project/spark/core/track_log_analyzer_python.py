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
        .setAppName("Python Track Log Analyzer Application")\
        .setMaster("local[2]")

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)

    """
        Step 1:
            read data：SparkContext 用户读取数据
    """
    track_log = "/user/hive/warehouse/db_track.db/yhd_log/"
    track_rdd = sc.textFile(track_log)

    # print count
    # print "COUNT = " + str(track_rdd.count())
    # print track_rdd.first()

    """
        Step 2:process data
            RDD#Transformation
        需求：
            统计每日的PV和UV
                PV：页面的访问数/浏览量
                    pv = COUNT(url)   url 不能为空, url.length > 0  第2列
                UV: 访客数
                    uv = COUNT(DISTINCT guid)  第6列
                时间：
                    用户访问网站的时间字段
                    tracktime  2015-08-28 18:10:00  第18列
    """
    # 字符分割的映射函数
    def split_data_func(line):
        # 字符串分割
        words = line.split("\t")
        # 字符串的截取
        date_str = str(words[17])[0:10]
        # return (date, url, uid)
        return date_str, words[1], words[5]

    # 清洗过滤数据
    filtered_rdd = track_rdd\
        .filter(lambda line: (len(line.strip()) > 0) and (len(line.split("\t")) > 20))\
        .map(split_data_func)

    # 打印第一元素
    # print str(filtered_rdd.first())
    # ('2015-08-28', u'http://www.yhd.com/?union_ref=7&cp=0', u'PR4E9HWE38DMN4Z6HUG667SCJNZXMHSPJRER')

    # 统计每日PV
    pv_rdd = filtered_rdd\
        .map(lambda (date, url, uid): (date, url))\
        .filter(lambda t: len(t[1].strip()) > 0)\
        .map(lambda t: (t[0], 1))\
        .reduceByKey(lambda x, y: x + y)
    # print
    pv_first = pv_rdd.first()
    print "PV: " + str(pv_first[0]) + " = " + str(pv_first[1])
    # PV: 2015-08-28 = 69197

    # 统计每日UV
    uv_rdd = filtered_rdd\
        .map(lambda (date, url, uid): (date, uid))\
        .distinct() \
        .map(lambda t: (t[0], 1)) \
        .reduceByKey(lambda x, y: x + y)
    uv_first = uv_rdd.first()
    print "UV: " + str(uv_first[0]) + " = " + str(uv_first[1])
    # UV: 2015-08-28 = 39007

    # union: 将RDD进行合并
    union_rdd = pv_rdd.union(uv_rdd)
    print union_rdd.collect()

    # join: 将RDD进行关联，RDD的数据类型为两元组
    join_rdd = pv_rdd.join(uv_rdd)
    print join_rdd.collect()

    """
        作业：
            将结果集写入到MySQK数据库中
            针对Python来说需要安装mysql-python

            如何安装Python中第三方模块
            - pip 命令安装第三方
            1. 在线安装
                - pip install [第三方包的名称]
            2. 离线安装
                - pip install xxx.whl
    """

    # 休眠一段时， 为了WEB UI进行监控
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()

