package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task2aSql {

  def main(args: Array[String]): Unit = {
    println("Starting app...")

    val spark = SparkSession.builder.appName("Task 2a").master("local[*]").getOrCreate()

    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      .createOrReplaceTempView("events")

    val categoryMedianSessionDuration=spark.sql("select category, percentile_approx(sessionDurationMs, 0.5) as categoryMedianSessionDuration " +
      "from (select distinct category, sessionId, unix_timestamp(sessionEndTime) - unix_timestamp(sessionStartTime) as sessionDurationMs " +
      "from (select category, product, userId, eventTime, eventType, sessionId, " +
      "min(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime, " +
      "max(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime " +
      "from (select category, product, userId, eventTime, eventType, " +
      "concat((sum(cast ((coalesce(unix_timestamp(eventTime) - unix_timestamp(lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) > 300) as bigint)) OVER (PARTITION BY userId, category ORDER BY eventTime)), '-', userId, '-', category) as sessionId " +
      "from events) tmp) tmp1) tmp2 group by category")

    categoryMedianSessionDuration.show(30)

    spark.stop()
  }
}