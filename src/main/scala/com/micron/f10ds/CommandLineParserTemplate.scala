package com.micron.f10ds

import java.io.File

import com.typesafe.config.ConfigFactory
//https://github.com/scallop/scallop/releases

import org.rogach.scallop._
/*
* This is a template code to record how to do
*
*
 */

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String]("input", required = true)
  verify()
}

object CommandLineParserTemplate {


  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val input = conf.input()
    println(input)

    val myConfigFile = new File(input)
    val fileConfig = ConfigFactory.parseFile(myConfigFile)
    val config = ConfigFactory.load(fileConfig)
    val value = config.getString("my")
    //    val value = ConfigFactory.load().getString("my")
    println(s"My secret value is $value")




  }
}

