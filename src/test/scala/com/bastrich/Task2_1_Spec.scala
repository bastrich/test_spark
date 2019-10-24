package com.bastrich

import com.bastrich.utils.BaseSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class Task2_1_Spec
  extends BaseSpec {

  it("test find category median sessions") {
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

    val actualResultDf = task2_1.calculateCategoryMedianSessionDuration(testSourceDf)

    actualResultDf.show(30, false)
    assertSmallDataFrameEquality(actualResultDf, expectedResultDf)
  }

  it("test wrong input data schema") {
    val task = new Task2_1
    testWrongInputSchema(task.calculateCategoryMedianSessionDuration)
  }
}
