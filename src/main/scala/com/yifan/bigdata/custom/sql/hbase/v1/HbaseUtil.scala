package com.yifan.bigdata.custom.sql.hbase.v1

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtil {
  def printHbseResult(result: Result): Unit = {
    println("new result")
    val cells = result.rawCells()

    for (cell <- cells) {
      println(Bytes.toString(result.getRow) + " \t"
        + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
        + Bytes.toString(CellUtil.cloneValue(cell)))
    }

  }
}
