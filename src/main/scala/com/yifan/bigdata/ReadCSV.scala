package com.yifan.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadCSV").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val customSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("major", StringType, true)
    ))
    //    val df = spark.read.format("csv").schema(customSchema).load("src/main/resources/student.csv")
    val df = spark.read.format("csv").load("src/main/resources/student.csv")
    df.show()
  }

}
