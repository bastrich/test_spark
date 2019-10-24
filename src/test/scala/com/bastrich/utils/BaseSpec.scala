package com.bastrich.utils

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec

class BaseSpec
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  protected def testWrongInputSchema(t: (DataFrame, Int) => DataFrame): Unit = {
    val wrongSchema = List(
      StructField("category", IntegerType),
      StructField("product", IntegerType)
    )
    val wrongData = Seq(
      Row(1, 2),
      Row(3, 4)
    )
    val wrongInputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(wrongData),
      StructType(wrongSchema)
    )

    assertThrows[Exception] {
      t(wrongInputDf, 300)
    }
  }

  protected def testWrongInputSchemaPureDf(t: DataFrame => DataFrame): Unit = {
    testWrongInputSchema((df: DataFrame, _: Int) => {
      t(df)
    })
  }
}
