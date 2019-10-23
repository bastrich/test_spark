package com.bastrich

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.bastrich.utils.Task1BaseSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.FunSpec

class Task1_2_Spec
  extends Task1BaseSpec {

  it("test enriching with sessions") {
    val task = new Task1_2
    testEnrichingWithSessions(task.enrichWithSessionIds)
  }
}
