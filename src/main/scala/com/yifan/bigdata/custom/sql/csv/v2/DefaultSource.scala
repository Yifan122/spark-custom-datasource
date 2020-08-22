package com.yifan.bigdata.custom.sql.csv.v2

import java.util
import java.util.Optional

import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode}

class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = new CSVDataSourceReader(options.get("path").get())

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???
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