package com.bastrich.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Utils {
  def verifyScheme(actualScheme: StructType): Boolean = {
    StructType(List(
      StructField("category", StringType),
      StructField("product", StringType),
      StructField("userId", StringType),
      StructField("eventTime", TimestampType),
      StructField("eventType", StringType)
    )).equals(actualScheme)
  }
}
