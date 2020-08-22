package com.yifan.bigdata.custom.sql.csv.v2

import org.apache.spark.sql.SparkSession

object DataSourceV2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("com.yifan.bigdata.custom.sql.csv.v2")
      .option("path", "data/student.csv")
      .load()

    df.show()

  }
}
