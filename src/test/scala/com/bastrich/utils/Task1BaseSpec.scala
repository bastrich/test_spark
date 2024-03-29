package com.bastrich.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}

class Task1BaseSpec
  extends BaseSpec {

  protected def testEnrichingWithSessions(t: (DataFrame, Int) => DataFrame): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val expectedSchema = List(
      StructField("category", StringType),
      StructField("product", StringType),
      StructField("userId", StringType),
      StructField("eventTime", TimestampType),
      StructField("eventType", StringType),
      StructField("sessionId", StringType),
      StructField("sessionStartTime", TimestampType),
      StructField("sessionEndTime", TimestampType)
    )
    val expectedData = Seq(
      Row("c1", "p1", "u1", new Timestamp(dateFormat.parse("2018-03-01 12:00:02").getTime), "e1", "0-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 12:00:02").getTime), new Timestamp(dateFormat.parse("2018-03-01 12:01:40").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 12:01:40").getTime), "e2", "0-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 12:00:02").getTime), new Timestamp(dateFormat.parse("2018-03-01 12:01:40").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 12:10:50").getTime), "e2", "1-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 12:10:50").getTime), new Timestamp(dateFormat.parse("2018-03-01 12:11:05").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 12:11:05").getTime), "e3", "1-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 12:10:50").getTime), new Timestamp(dateFormat.parse("2018-03-01 12:11:05").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 13:10:50").getTime), "e2", "2-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 13:10:50").getTime), new Timestamp(dateFormat.parse("2018-03-01 13:11:05").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 13:11:05").getTime), "e3", "2-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 13:10:50").getTime), new Timestamp(dateFormat.parse("2018-03-01 13:11:05").getTime)),
      Row("c1", "p2", "u1", new Timestamp(dateFormat.parse("2018-03-01 13:11:05").getTime), "e1", "2-u1-c1", new Timestamp(dateFormat.parse("2018-03-01 13:10:50").getTime), new Timestamp(dateFormat.parse("2018-03-01 13:11:05").getTime)),
      Row("c1", "p2", "u2", new Timestamp(dateFormat.parse("2018-03-01 15:10:00").getTime), "e4", "0-u2-c1", new Timestamp(dateFormat.parse("2018-03-01 15:10:00").getTime), new Timestamp(dateFormat.parse("2018-03-01 15:10:00").getTime))
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

    val actualResultDf = t(testSourceDf, 300)

    actualResultDf.show(30, false)
    assertSmallDataFrameEquality(actualResultDf, expectedResultDf)
  }
}
