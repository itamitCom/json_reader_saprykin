package com.example.json_reader_saprykin

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleSparkJob extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Example app")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val data: Array[Int] = Array(1, 2, 3, 4, 5)

  val firstRDD: RDD[Int] = sc.parallelize(data)

  val result = firstRDD
    .map(x => x + 1)
    .flatMap(x => List(x, x*2, x*3))
    .filter(x => x > 3)
    .collect()

  println(result.toList)
}
