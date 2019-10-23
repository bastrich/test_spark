package com.bastrich

import org.apache.spark.sql.DataFrame

class Task2_1 {

  def calculateCategoryMedianSessionDuration(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      """
        |select category, percentile_approx(sessionDurationMs, 0.5) as categoryMedianSessionDuration
        |from (select distinct category,
        |                      sessionId,
        |                      unix_timestamp(sessionEndTime) - unix_timestamp(sessionStartTime) as sessionDurationMs
        |      from (select category,
        |                   product,
        |                   userId,
        |                   eventTime,
        |                   eventType,
        |                   sessionId,
        |                   min(eventTime)
        |                   OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime,
        |                   max(eventTime)
        |                   OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime
        |            from (select category,
        |                         product,
        |                         userId,
        |                         eventTime,
        |                         eventType,
        |                         concat((sum(cast((coalesce(unix_timestamp(eventTime) - unix_timestamp(
        |                                 lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) >
        |                                           300) as bigint))
        |                                 OVER (PARTITION BY userId, category ORDER BY eventTime)), '-', userId, '-',
        |                                category) as sessionId
        |
        |                  from events)))
        |group by category
        |""".stripMargin)
  }
}