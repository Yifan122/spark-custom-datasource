package com.yifan.bigdata

import org.apache.spark.sql.SparkSession

object DataSourceV1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadCSV").master("local[2]").getOrCreate()
    val df = spark.read.format("csv").load("src/main/resources/student.csv")
    df.show()
  }
}
