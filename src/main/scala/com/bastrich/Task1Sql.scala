package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task1Sql {

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

    val spark = SparkSession.builder.appName("Task 1 SQL").master("local[*]").getOrCreate()

    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      .createOrReplaceTempView("events")

    //sessionId is <session number within given user and category>-<userId>-<category> (in order to distinct sessionIds globally)
    val df=spark.sql("select category, product, userId, eventTime, eventType, sessionId, " +
      "min(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime, " +
    "max(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime " +
      "from (select category, product, userId, eventTime, eventType, " +
      "concat((sum(cast ((coalesce(unix_timestamp(eventTime) - unix_timestamp(lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) > 300) as bigint)) OVER (PARTITION BY userId, category ORDER BY eventTime)), '-', userId, '-', category) as sessionId " +
      "from events) tmp")

    df.show(30)

    spark.stop()
  }
}