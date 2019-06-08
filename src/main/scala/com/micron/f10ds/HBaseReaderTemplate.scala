package com.micron.f10ds

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client._

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.filter._
import scala.collection.JavaConverters._
import java.io.File
import com.typesafe.config.ConfigFactory

/**
 * Hello world!
 *
 */


// First
object HBaseReaderTemplate{
  val conf: Configuration = HBaseConfiguration.create()

  def printRow(result : Result) = {
    val cells = result.rawCells()
    print( Bytes.toString(result.getRow) + " : " )
    for(cell <- cells){
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }

  def main(args:Array[String]){

    //conf.set("hbase.master", "<server>.hortonworks.com" + ":" + "60000")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "slave01,master01,slave02")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure") // IMPORTANT!!!
    conf.setInt("hbase.client.scanner.caching", 10000)
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("eng_hbase_table_01:test"))
    println("connection created")

    // Get example
    println("Get Example:")
    var get = new Get(Bytes.toBytes("test_row1"))
    var result = table.get(get)
    printRow(result)


    //Scan example
    println("\nScan Example:")
    val dataScan = new Scan()

//    dataScan.setFilter(filter)
    dataScan.setCaching(5000)
    val startRow=Bytes.toBytes("test_row2")
    val stopRow=Bytes.toBytes("test_row2z")
    dataScan.withStartRow(startRow)
    dataScan.withStopRow(stopRow)
    var dataResult = table.getScanner(dataScan)

    dataResult.asScala.foreach(result => {
      printRow(result)
    })


    connection.close()
    println("Hello Scala")
  }
}