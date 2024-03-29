package com.bastrich

import com.bastrich.utils.BaseSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class Task2_3_Spec
  extends BaseSpec {

  it("test find category top products by spending time") {
    val expectedSchema = List(
      StructField("category", StringType),
      StructField("product", StringType),
      StructField("rank", IntegerType)
    )
    val expectedData = Seq(
      Row("c1", "p1", 1),
      Row("c1", "p2", 2),
      Row("c2", "p3", 1)
    )
    val expectedResultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val testSourceDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load(getClass.getResource("/test_data_3.csv").toURI.getPath)

    val task2_3 = new Task2_3

    val actualResultDf = testSourceDf.transform(task2_3.findCategoryProductsTop)

    actualResultDf.show(30, false)
    assertSmallDataFrameEquality(actualResultDf, expectedResultDf)
  }

  it("test wrong input data schema") {
    val task = new Task2_3
    testWrongInputSchemaPureDf(task.findCategoryProductsTop)
  }
}
