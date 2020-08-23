package com.yifan.bigdata.custom.sql.hbase.v1

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class HBaseDatasourceRelation(override val sqlContext: SQLContext,
                              val userSchema: StructType) extends BaseRelation
  with TableScan {
  override def schema: StructType = {
    if (userSchema != null) {
      // The user defined a schema, simply return it
      userSchema
    } else {
      // There is no user-defined schema.
      StructType(
        StructField("name", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) ::
          StructField("major", StringType, nullable = true) ::
          Nil
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    val TABLE_NAME: String = "student"


    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "localhost:2181")
    configuration.set(TableInputFormat.INPUT_TABLE, TABLE_NAME) //指定要查询表名
    configuration.set(TableInputFormat.SCAN_COLUMNS, "f:name f:age f:major")

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val result: RDD[Row] = hbaseRdd.map(_._2).map(result => {
      Row(
        Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("name"))),
        Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("age")))),
        Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("major")))
      )
    })

    result
  }
}
