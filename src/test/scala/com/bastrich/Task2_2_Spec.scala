package com.bastrich

import java.text.SimpleDateFormat

import com.bastrich.utils.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class Task2_2_Spec
  extends FunSpec
      with SparkSessionTestWrapper
      with DataFrameComparer {

    it("test find user segments sizes") {
      val expectedSchema = List(
        StructField("category", StringType),
        StructField("segment", StringType, false),
        StructField("uniqueUsersCount", LongType, false)
      )
      val expectedData = Seq(
        Row("c2", "moreThan5m", 1L),
        Row("c1", "lessThan1m", 1L),
        Row("c1", "from1mTo5m", 1L),
        Row("c1", "moreThan5m", 2L),
        Row("c2", "from1mTo5m", 1L)
      )
      val expectedResultDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val testSourceDf = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(getClass.getResource("/test_data_2.csv").toURI.getPath)

      val task2_2 = new Task2_2

      val actualResultDf = task2_2.findSegmentsSizes(testSourceDf)

      actualResultDf.show(30, false)
      assertSmallDataFrameEquality(actualResultDf, expectedResultDf)
    }
  }
