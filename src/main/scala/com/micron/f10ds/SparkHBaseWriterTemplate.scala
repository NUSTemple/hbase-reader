package com.micron.f10ds

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.internal.Logging

object SparkHBaseWriterTemplate {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    // define
    val sparkAppName = "SparkHBaseWriterExample"
    val master = "local[2]"
    val tableName = "eng_hbase_table_sales:records"
    val cfamily: String = "cf"
    val inputPath = "file:///home/pengtan/Downloads/data.csv"

    val spark = SparkSession
      .builder
      .appName(sparkAppName)
      .master(master)
      .getOrCreate

    val sc = spark.sparkContext
    val t = sc.textFile(inputPath).repartition(4)
    val headers = t.first.split(",").toList
    val rowkeyIndex = 6

    t.foreachPartition { iter =>
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.addResource("file:///home/pengtan/Downloads/hbase-site.xml")
        hbaseConf.set("hbase.zookeeper.quorum","master01,slave01,slave02")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

        hbaseConf.set("hbase.master", "192.168.1.169:16000")
        hbaseConf.set("zookeeper.session.timeout", "300000")
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

        val connection = ConnectionFactory.createConnection(hbaseConf)

        val table = connection.getTable(TableName.valueOf(tableName))
        iter.foreach { a =>


          val arr = a.split(",")
          val p = new Put(Bytes.toBytes(arr(rowkeyIndex)))
          for (i <- headers.indices ) {
            p.addColumn(Bytes.toBytes(cfamily), Bytes.toBytes(headers(i)), Bytes.toBytes(arr(i)))
          }

          table.put(p)
        }

    }
    println("spark complete!")
    spark.stop()

  }

}
