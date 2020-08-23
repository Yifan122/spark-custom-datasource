package com.yifan.bigdata.custom.sql.hbase.v1

import org.apache.spark.sql.SparkSession

object HBaseDatasourceV1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("com.yifan.bigdata.custom.sql.hbase.v1")
      .load()

    df.show()
  }

}
