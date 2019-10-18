package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {
    println("Hello from main of class")

    val window1 = Window.partitionBy("userId", "category").orderBy("eventTime")
    val window2 = Window.partitionBy("userId", "category", "sessionId").orderBy("eventTime")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )

    val newSession = (coalesce(
      unix_timestamp(col("eventTime")) - unix_timestamp(lag(col("eventTime"), 1).over(window1)),
      lit(0)
    ) > 300).cast("bigint")


    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")
      //      .withColumn("session", lit("aaaaa"))
      .withColumn("diff_prev_eventTime", unix_timestamp(col("eventTime")) - unix_timestamp(lag(col("eventTime"), 1).over(window1)))
      //            .withColumn("diff_prev_eventTime", when(col("diff_prev_eventTime").isNull, 0).otherwise(col("diff_prev_eventTime")))

      //      .withColumn("newsession",
      //        MyUDWF.calculateSession(col("eventTime"), col("session")) over window1
      //      )

      .withColumn("sessionId", concat(col("userId"), lit("-session"), sum(newSession).over(window1)))
      .withColumn("sessionStartTime", min(col("eventTime")).over(window2))
      .withColumn("sessionEndTime", max(col("eventTime")).over(window2))

    df.show(28)
    //    df.printSchema()

    val categoryMedianSessionDuration = df
      .withColumn("sessionDurationMs", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
      .select("category", "sessionId", "sessionDurationMs")
      .dropDuplicates()
      .groupBy("category")
      .agg(expr("percentile_approx(sessionDurationMs, 0.5)").alias("categoryMedianSessionDuration"))

    categoryMedianSessionDuration.show(30)

    val window3 = Window.partitionBy("category", "segment")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )
    val usersDistribution = df
      .withColumn("sessionDurationMs", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
      .select("category", "userId", "sessionDurationMs")
      .dropDuplicates()
      .withColumn("segment", when(col("sessionDurationMs") < 60, lit("less1m")).otherwise(when(col("sessionDurationMs") <= 300, lit("1mto5m")).otherwise(lit("more5m"))))
      .withColumn("users_count", count("userId").over(window3))
      .select("category", "segment", "users_count")


    usersDistribution.show(30)

    val window4 = Window.partitionBy("category", "product", "userId").orderBy("eventTime")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )
    val window5 = Window.partitionBy("category").orderBy(col("spentTime").desc)
//      .rangeBetween(
//        Window.unboundedPreceding,
//        Window.unboundedFollowing
//      )
    val productTime = df
      .withColumn("productStartTime", min(col("eventTime")).over(window4))
      .withColumn("productEndTime", max(col("eventTime")).over(window4))
        .withColumn("spentTime", unix_timestamp(col("productEndTime")) - unix_timestamp(col("productStartTime")))
        .select("category", "product", "spentTime")
        .dropDuplicates()
        .withColumn("rank", dense_rank().over(window5))
    .select("category", "rank", "product", "spentTime")


    productTime.show(30)


    spark.stop()
  }
}