package com.bastrich

import org.apache.spark.sql.{DataFrame, SparkSession}

class Task1_1 {
  def enrichWithSessionIds(df: DataFrame, sessionExpirationSeconds: Int = 300): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      s"""
        |SELECT category,
        |       product,
        |       userId,
        |       eventTime,
        |       eventType,
        |       sessionId,
        |       min(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime,
        |       max(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime
        |FROM (SELECT category,
        |             product,
        |             userId,
        |             eventTime,
        |             eventType,
        |             concat(
        |                 (sum(cast((coalesce(unix_timestamp(eventTime) - unix_timestamp(lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) > $sessionExpirationSeconds) as bigint))
        |                     OVER (PARTITION BY userId, category ORDER BY eventTime)),
        |                 '-',
        |                 userId,
        |                 '-',
        |                 category) as sessionId
        |      FROM events)
        |""".stripMargin)
  }
}