package com.bastrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task2_3 {

  def main(args: Array[String]): Unit = {
    println("Starting app...")

    val spark = SparkSession.builder.appName("Task 2c").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data.csv")

    df.show(28)

    val productSessionWindow = Window.partitionBy("category", "product", "userId").orderBy("eventTime")
      .rangeBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
      )
    val categoryWindow = Window.partitionBy("category").orderBy(col("spentTime").desc)
    val productTime = df
      .withColumn("productStartTime", min(col("eventTime")).over(productSessionWindow))
      .withColumn("productEndTime", max(col("eventTime")).over(productSessionWindow))
      .withColumn("spentTime", unix_timestamp(col("productEndTime")) - unix_timestamp(col("productStartTime")))
      .select("category", "product", "spentTime")
      .dropDuplicates()
      .withColumn("rank", dense_rank().over(categoryWindow))
      .select("category", "rank", "product", "spentTime")

    productTime.show(30)

    spark.stop()
  }
}