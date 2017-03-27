package com.ibeifeng.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object TestSpark {
 
   /**
    * Spark Application  Running Entry
    *
    * Driver Program
    * @param args
    */
   def main(args: Array[String]) {
     // Create SparkConf Instance
     val sparkConf = new SparkConf()
         // 设置应用的名称, 4040监控页面显示
         .setAppName("LogAnalyzerSpark Application")
         // 设置程序运行环境, local
         .setMaster("local[2]")
 
     // Create SparkContext
     /**
      * -1,读取数据，创建RDD
      * -2,负责调度Job和监控
      */
     val sc = new SparkContext(sparkConf)


     val trackRdd = sc.textFile("/user/hive/warehouse/db_track.db/yhd_log/date=20150828/")



















     // RDD Cache
     println(trackRdd.count())
     println(trackRdd.first())


 /**  ======================================================================= */
     // SparkContext Stop
     sc.stop()
   }
 
 }
