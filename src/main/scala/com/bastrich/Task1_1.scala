package com.bastrich

import org.apache.spark.sql.{DataFrame, SparkSession}

class Task1_1 {
  def enrichWithSessionIds(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      """
        |select category,
        |       product,
        |       userId,
        |       eventTime,
        |       eventType,
        |       sessionId,
        |       min(eventTime)
        |       OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime,
        |       max(eventTime)
        |       OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime
        |from (select category,
        |             product,
        |             userId,
        |             eventTime,
        |             eventType,
        |             concat((sum(cast((coalesce(unix_timestamp(eventTime) - unix_timestamp(
        |                     lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) > 300) as bigint))
        |                     OVER (PARTITION BY userId, category ORDER BY eventTime)), '-', userId, '-', category) as sessionId
        |      from events) tmp
        |""".stripMargin)
  }
}