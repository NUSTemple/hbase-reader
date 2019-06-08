package com.micron.f10ds


import org.apache.spark.internal.Logging

object LoggerTemplate extends Logging{
  def main(args: Array[String]): Unit = {

//    logger.info("Test1")

    logInfo("Test2")


    logError("Test3")



  }

}
