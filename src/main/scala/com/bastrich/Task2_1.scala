package com.bastrich

import org.apache.spark.sql.DataFrame

class Task2_1 {

  def calculateCategoryMedianSessionDuration(df: DataFrame, sessionExpirationSeconds: Int = 300): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      s"""
        |WITH sessionIds AS (SELECT category,
        |                           product,
        |                           userId,
        |                           eventTime,
        |                           eventType,
        |                           concat((sum(cast((coalesce(unix_timestamp(eventTime) - unix_timestamp(lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)), 0) > $sessionExpirationSeconds) as bigint))
        |                                   OVER (PARTITION BY userId, category ORDER BY eventTime)),
        |                                  '-',
        |                                  userId,
        |                                  '-',
        |                                  category) as sessionId
        |                    from events),
        |
        |     sessionPeriods AS (SELECT category,
        |                               product,
        |                               userId,
        |                               eventTime,
        |                               eventType,
        |                               sessionId,
        |                               min(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime,
        |                               max(eventTime) OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime
        |                        FROM sessionIds)
        |
        |SELECT category, percentile_approx(sessionDuration, 0.5) as categoryMedianSessionDurationSeconds
        |FROM (SELECT DISTINCT category,
        |                      sessionId,
        |                      unix_timestamp(sessionEndTime) - unix_timestamp(sessionStartTime) as sessionDuration
        |      FROM sessionPeriods)
        |GROUP BY category
        |""".stripMargin)
  }
}