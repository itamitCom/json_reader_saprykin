package com.example.json_reader_saprykin

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonReader extends App {

  case class Wine(id: Int, country: String, points: Int, title: String, variety: String, winery: String)

  val pathToFile = args(0)

  val spark: SparkSession = SparkSession.builder()
    .appName("Json Reader App")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val sourceRDD = sc.textFile(s"$pathToFile")

  val result = sourceRDD.map(line => {
    parse(line)
  }).map(json => {
    implicit lazy val formats = org.json4s.DefaultFormats
    val id = (json \ "id").extract[Int]
    val country = (json \ "country").extract[String]
    val points = (json \ "points").extract[Int]
    val title = (json \ "title").extract[String]
    val variety = (json \ "variety").extract[String]
    val winery = (json \ "winery").extract[String]

    val w = Wine(id, country, points, title, variety, winery)

    val j = pretty(render(Extraction.decompose(w)))
    println(s"json:\n$j")

    val decodedWine = parse(j).extract[Wine]
    println(s"decoded wine: $decodedWine")

  }).collect()
}
