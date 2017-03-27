package com.ibeifeng.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

class CombineByKeySpark

/**
 * RDD中combineByKey函数的使用
 */
object CombineByKeySpark {
  def main(args: Array[String]) {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName(classOf[CombineByKeySpark].getSimpleName)
      .setMaster("local[5]")

    // 创建SparkContext
    val sc = new SparkContext(sparkConf)

    /**
     * 说明一：
     *    combineByKey是Spark中一个比较核心的高级函数，其他一些高阶键值对函数底层
     * 都是用它实现的。诸如 groupByKey,reduceByKey, aggregateByKey等等。
     *    Spark 1.6开始，更名为combineByKeyWithClassTag
     *
     * 说明二：
     *     函数定义，以最简单的为例
     *    class PairRDDFunctions[K, V](self: RDD[(K, V)])
     *    def combineByKey[C](
     *      createCombiner: V => C,
     *      mergeValue: (C, V) => C,
     *      mergeCombiners: (C, C) => C
     *    ): RDD[(K, C)]
     *
     *  第一、要理解combineByKey()，要先理解它在处理数据时是如何处理每个元素的。由于combineByKey()
     *    会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的键相同.
     *  第二、参数函数说明：
     *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
     *    -1, 这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回
     *    (这一步类似于初始化操作).
     *    -2, 如果是一个新的元素，此时使用createCombiner()来创建那个键Key对应的累加器的初始值。
     *    （！注意：这个过程会在每个分区第一次出现各个键时发生，而不是在整个RDD中第一次出现一个键时发生。）
     *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
     *    -1, mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上
     *    (这个操作在每个分区内进行)
     *    -2, 如果这是一个在处理当前分区中之前已经遇到键，此时combineByKey()使用mergeValue()
     *    将该键的累加器对应的当前值与这个新值进行合并。
     *  - `mergeCombiners`, to combine two C's into a single one.
     *    -1, mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
     *    -2, 由于每个分区都是独立处理的，因此对于同一个键可以有多个累加器。如果有两个或者更多的
     *    分区都有对应同一个键的累加器，就需要使用用户提供的mergeCombiners()将各个分区的结果进行合并。
     */
    val initialScores = Array(
      ("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),
      ("Wilma", 95.0), ("Wilma", 98.0)
    )
    // 创建RDD
    val scoresRDD: RDD[(String, Double)] = sc.parallelize(initialScores, 2)
    // 针对每个人的各个学科成绩进行合并
    val combineScoresRdd: RDD[(String, (Double, Int))] = scoresRDD.combineByKey(
      // createCombiner: V => C
      score => (score, 1), // 针对每个分区中第一次处理的Key，进行combiner初始化
      // mergeValue: (C, V) => C,  // 每个分区Key的Value进行combiner
      (tuple: (Double, Int), score: Double) => (tuple._1 + score, tuple._2 + 1),
      // mergeCombiners: (C, C) => C,  // 不同分区的Combiner进行合并
      (t1: (Double, Int), t2: (Double, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    // 计算每个人的各个学科的平均值
    val avgScoresArray: Array[(String, Double)] = combineScoresRdd.map{
      case(name, (scores, count)) =>  (name, scores / count)
    }.collect()

    // 打印结果
    avgScoresArray.foreach(println)

    // WEB UI
    Thread.sleep(10000000)

    // SparkContext Close
    sc.stop()
  }

}
