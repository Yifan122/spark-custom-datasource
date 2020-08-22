package com.yifan.bigdata.custom.sql.csv.v2

import java.util

import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object CSVSourceV2 {
  val schema = StructType(
    StructField("name", StringType, nullable = true) ::
      StructField("surname", StringType, nullable = true) ::
      StructField("salary", IntegerType, nullable = true) ::
      Nil
  )
}


class CSVDataSourceReader(path: String) extends DataSourceReader {
  val requiredSchema = CSVSourceV2.schema

  override def readSchema(): StructType = requiredSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val list = new util.ArrayList[DataReaderFactory[Row]]()
    list.add(new CSVDataReaderFactory(path))

    list
  }
}

class CSVDataReaderFactory(path: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new CSVDataReader(path)
}

class CSVDataReader(path: String) extends DataReader[Row] {
  var iterator: Iterator[String] = null

  override def next(): Boolean = {
    if (iterator == null) {
      iterator = Source.fromFile(path).getLines.toList.iterator
    }
    iterator.hasNext
  }

  override def get(): Row = {
    val line = iterator.next().split("\\$")
    Row(line(0), line(1), line(2).toInt)
  }

  override def close(): Unit = {

  }
}

object CSVExample {
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