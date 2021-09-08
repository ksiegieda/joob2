package com.example

import com.mapr.db.spark.sql.toSparkSessionFunctions
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("dumb-tests").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val bData = spark.loadFromMapRDB("/tables/movie")
    bData.selectExpr("CAST(_id AS STRING) AS key", "CAST(_id AS STRING) AS value")
      .write
      .format("kafka")
      .option("topic", "/apps/stream:read")
      .save()

    spark.stop()
  }
}
