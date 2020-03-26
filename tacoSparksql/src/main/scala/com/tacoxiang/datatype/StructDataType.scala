package com.tacoxiang.datatype

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
 * ****************************************************************************************                                                 
 * Programs are meant to be read by humans and only incidentally for computers to execute
 * ****************************************************************************************
 * Author : tacoxiang                          
 * Time : 2020/3/26                            
 * Package : com.tacoxiang.datatype      
 * ProjectName: Bigdata
 * Describe :
 * ============================================================
 **/
object StructDataType {

  def getSparkContent: SparkContext = {
    val spark = SparkSession.builder().appName("test-spark-struct").master("local[2]").getOrCreate()
    spark.sparkContext
  }

  def getSqlContext: SQLContext = {
    val spark = SparkSession.builder().appName("test-spark-struct").master("local[2]").getOrCreate()
    spark.sqlContext
  }

  def getSchema: StructType = {
    StructType(
      Array(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      )
    )
  }

  def dataHandler(): Unit = {
    val sc = getSparkContent
    val tacoSqlContext = getSqlContext
    val schema = getSchema
    val people = sc.textFile("/Users/tacoxiang/Documents/code/github/Bigdata/" +
      "tacoSparksql/src/main/resources/experiment/people")
    val rowRdd = people.map(_.split(" ")).map(element => Row(element(0), element(1).trim.toInt))
    val peopleDf = tacoSqlContext.createDataFrame(rowRdd, schema)

    // 按照分组取组内第一个
    /**
     * +------+--------+
     * |name  |firstAge|
     * +------+--------+
     * |taco  |29      |
     * |carole|30      |
     * |moss  |28      |
     * +------+--------+
     */
    peopleDf.groupBy("name").agg(
      first(peopleDf("age")).alias("firstAge")
    ).show(false)

    /**
     * +------+---+-------------+
     * |name  |age|tenYearsLater|
     * +------+---+-------------+
     * |taco  |29 |39           |
     * |taco  |28 |38           |
     * |taco  |27 |37           |
     * |moss  |28 |38           |
     * |moss  |27 |37           |
     * |carole|30 |40           |
     * |carole|29 |39           |
     * +------+---+-------------+
     */
    peopleDf.withColumn("tenYearsLater", col("age").plus(10)).show(false)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    dataHandler()
  }
}
