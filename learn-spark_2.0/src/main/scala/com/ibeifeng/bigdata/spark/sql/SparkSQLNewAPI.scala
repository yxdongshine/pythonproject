package com.ibeifeng.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark 2.x 中SparkSession的API使用
 */
object SparkSQLNewAPI {

  def main(args: Array[String]) {

    // A SparkSession
    val spark = SparkSession
      .builder() // a builder pattern
      .appName("SparkSQLNewAPI Application")
      .master("local[5]")
      .config("spark.sql.warehouse.dir", "/user/beifeng/spark-warehouse")
      .getOrCreate()

    /**
      val sqlContext = spark.sqlContext
      val sc = spark.sparkContext
    */
    val jsonDF: DataFrame = spark.read.json("/examples/src/main/resources/people.json")

    jsonDF.printSchema()
    jsonDF.show()

    //
    jsonDF.createOrReplaceTempView("view_people")
    // SQL
    spark.sql(
      """
        |SELECT * FROM view_people
      """.stripMargin).show()

    /**
     * 支持读取CSV格式的文件
     */
    val appleStockDF: DataFrame = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/spark/apple-stock.csv")

    appleStockDF.show(false)

    // 休眠一段时间
    Thread.sleep(1000000)

    //SparkSession Stop
    spark.stop()

  }

}
