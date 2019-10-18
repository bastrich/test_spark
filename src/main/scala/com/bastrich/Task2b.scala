package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task2b {

  def main(args: Array[String]): Unit = {
    println("Hello from main of class")

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

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      .withColumn("sessionId", concat(sum(sessionCol).over(categoryWindow), lit("-"), col("userId"), lit("-"), col("category")))
      .withColumn("sessionStartTime", min(col("eventTime")).over(sessionWindow))
      .withColumn("sessionEndTime", max(col("eventTime")).over(sessionWindow))

    df.show(30)

    val durationSegmentWindow = Window.partitionBy("category", "segment")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )
    val usersDistribution = df
      .withColumn("sessionDurationMs", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
      .select("category", "userId", "sessionDurationMs")
      .dropDuplicates()
      .withColumn("segment", when(col("sessionDurationMs") < 60, lit("less1m")).otherwise(when(col("sessionDurationMs") <= 300, lit("1mto5m")).otherwise(lit("more5m"))))
      .withColumn("users_count", count("userId").over(durationSegmentWindow))
      .select("category", "segment", "users_count")
      .dropDuplicates()

    usersDistribution.show(30)

    spark.stop()
  }
}