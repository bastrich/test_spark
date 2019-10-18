package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task2a {

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

    val spark = SparkSession.builder.appName("Task 2a").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      .withColumn("sessionId", concat(sum(sessionCol).over(categoryWindow), lit("-"), col("userId"), lit("-"), col("category")))
      .withColumn("sessionStartTime", min(col("eventTime")).over(sessionWindow))
      .withColumn("sessionEndTime", max(col("eventTime")).over(sessionWindow))

    df.show(30)

    val categoryMedianSessionDuration = df
      .withColumn("sessionDurationMs", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
      .select("category", "sessionId", "sessionDurationMs")
      .dropDuplicates()
      .groupBy("category")
      .agg(expr("percentile_approx(sessionDurationMs, 0.5)").alias("categoryMedianSessionDuration"))

    categoryMedianSessionDuration.show(30)

    spark.stop()
  }
}