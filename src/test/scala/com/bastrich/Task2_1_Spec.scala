package com.bastrich

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.bastrich.utils.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec

class Task2_1_Spec
  extends FunSpec
      with SparkSessionTestWrapper
      with DataFrameComparer {

    it("test find category median sessions") {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val expectedSchema = List(
        StructField("category", StringType),
        StructField("categoryMedianSessionDurationSeconds", DoubleType)
      )
      val expectedData = Seq(
        Row("c1", 15d)
      )
      val expectedResultDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val testSourceDf = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(getClass.getResource("/test_data_1.csv").toURI.getPath)

      val task2_1 = new Task2_1

      val actualResultDf = task2_1.calculateCategoryMedianSessionDuration(testSourceDf, 300)

      actualResultDf.show(30, false)
      assertSmallDataFrameEquality(actualResultDf, expectedResultDf)
    }
  }
