package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task1 {

  def main(args: Array[String]): Unit = {
    println("Starting app...")

    val categoryWindow = Window.partitionBy("userId", "category").orderBy("eventTime")
    val sessionWindow = Window.partitionBy("userId", "category", "sessionId").orderBy("eventTime")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )
    val sessionCol = (coalesce(
      unix_timestamp(col("eventTime")) - unix_timestamp(lag(col("eventTime"), 1).over(categoryWindow)),
      lit(0)
    ) > 300).cast("bigint")

    val spark = SparkSession.builder.appName("Task 1").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      //sessionId is <session number within given user and category>-<userId>-<category> (in order to distinct sessionIds globally)
      .withColumn("sessionId", concat(sum(sessionCol).over(categoryWindow), lit("-"), col("userId"), lit("-"), col("category")))
      .withColumn("sessionStartTime", min(col("eventTime")).over(sessionWindow))
      .withColumn("sessionEndTime", max(col("eventTime")).over(sessionWindow))

    df.show(30)

    spark.stop()
  }
}