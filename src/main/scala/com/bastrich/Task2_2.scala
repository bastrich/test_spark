package com.bastrich

import org.apache.spark.sql.DataFrame

class Task2_2 {

  def findSegmentsSizes(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      """
        |with sessionEnrichedEvents as (select category,
        |                                      product,
        |                                      userId,
        |                                      eventTime,
        |                                      eventType,
        |                                      sessionId,
        |                                      min(eventTime)
        |                                      OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionStartTime,
        |                                      max(eventTime)
        |                                      OVER (PARTITION BY userId, category, sessionId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sessionEndTime
        |                               from (select category,
        |                                            product,
        |                                            userId,
        |                                            eventTime,
        |                                            eventType,
        |                                            concat((sum(cast((coalesce(unix_timestamp(eventTime) - unix_timestamp(
        |                                                    lag(eventTime, 1) OVER (PARTITION BY userId, category ORDER BY eventTime)),
        |                                                                       0) >
        |                                                              43200) as bigint))
        |                                                    OVER (PARTITION BY userId, category ORDER BY eventTime)), '-',
        |                                                   userId, '-',
        |                                                   category) as sessionId
        |
        |                                     from events))
        |
        |SELECT category, segment, count(userId) as uniqueUsersCount
        |from (select distinct category,
        |                      userId,
        |                      CASE
        |                          WHEN sessionDuration < 60 THEN 'lessThan1m'
        |                          ELSE CASE
        |                                   WHEN sessionDuration >= 60 AND sessionDuration <= 300 THEN 'from1mTo5m'
        |                                   ELSE 'moreThan5m' END END as segment
        |      from (select distinct category,
        |                            userId,
        |                            sessionId,
        |                            unix_timestamp(sessionEndTime) - unix_timestamp(sessionStartTime) as sessionDuration
        |            from sessionEnrichedEvents))
        |group by category, segment
        |
        |""".stripMargin
    )
  }
}