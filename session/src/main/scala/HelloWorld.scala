package com.caimh.sparksql

import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.sql.SparkSession


object HelloWorld {

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .master("local[*]")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()
//
//    // For implicit conversions like converting RDDs to DataFrames
//    import spark.implicits._
//
//    val df = spark.read.json("E:\\people.json")
//
//    // Displays the content of the DataFrame to stdout
//    df.show()
//
//    df.filter($"age" > 21).show()
//
//    df.createOrReplaceTempView("persons")
//
//    spark.sql("SELECT * FROM persons where age > 21").show()
//
//    spark.stop()

    //2019-11-08
//    println(DateUtils.getTodayDate())

//    val builder = new StringBuilder()
//    println(builder.toString())
//    println(builder.toString().contains("abc"))

//    var parameter = "abcdefg|"
//    println(parameter.substring(0,parameter.length-1))
//    parameter = parameter.substring(0,parameter.length-1)
//    println(parameter)

    val time = "350"
    println(time.toLong)

  }

}
