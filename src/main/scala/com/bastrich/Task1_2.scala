package com.bastrich

import java.sql.Timestamp

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class Task1_2 {
  def enrichWithSessionIds(df: DataFrame, sessionExpirationSeconds: Int = 300): DataFrame = {
    df.createOrReplaceTempView("events")

    def findSession = (sessions: Map[Timestamp, String], eventTime: Timestamp) => {
      sessions.get(eventTime)
    }

    df.sparkSession.udf.register("sessions", new Sessions(sessionExpirationSeconds))
    df.sparkSession.udf.register("findSession", findSession)

    df.sqlContext.sql(
      """
        |WITH sessionAggs AS (SELECT category,
        |                            userId,
        |                            sessions(eventTime) as sessions
        |                     FROM (select * from events order by eventTime)
        |                     GROUP BY category, userId),
        |
        |     sessionIds AS (SELECT events.category,
        |                           events.product,
        |                           events.userId,
        |                           events.eventTime,
        |                           events.eventType,
        |                           findSession(sessionAggs.sessions, events.eventTime) as sessionId
        |                    FROM events
        |                             JOIN
        |                         sessionAggs
        |                         ON events.category == sessionAggs.category AND events.userId == sessionAggs.userId),
        |
        |     minMaxSessionTimes AS (SELECT category,
        |                                   userId,
        |                                   sessionId,
        |                                   min(eventTime) as sessionStartTime,
        |                                   max(eventTime) as sessionEndTime
        |                            FROM sessionIds
        |                            GROUP BY category, userId, sessionId)
        |
        |SELECT sessionIds.category,
        |       sessionIds.product,
        |       sessionIds.userId,
        |       sessionIds.eventTime,
        |       sessionIds.eventType,
        |       concat(sessionIds.sessionId, '-', sessionIds.userId, '-', sessionIds.category) as sessionId,
        |       sessionStartTime,
        |       sessionEndTime
        |FROM sessionIds
        |         JOIN
        |     minMaxSessionTimes
        |     ON sessionIds.category = minMaxSessionTimes.category AND sessionIds.userId = minMaxSessionTimes.userId AND
        |        sessionIds.sessionId = minMaxSessionTimes.sessionId
        |""".stripMargin)
  }
}

class Sessions(val sessionExpirationSeconds: Int) extends UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", TimestampType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("currentSessionId", LongType) ::
      StructField("pastTimestamp", TimestampType) ::
      StructField("sessions", MapType(TimestampType, StringType)) :: Nil
  )

  override def dataType: DataType = MapType(TimestampType, StringType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = new Timestamp(0)
    buffer(2) = Map[Timestamp, String]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val timeDiff = if (buffer.getAs[Timestamp](1).getTime == 0) {
      0
    } else {
      input.getAs[Timestamp](0).getTime - buffer.getAs[Timestamp](1).getTime
    }

    val currentSessionId = buffer.getAs[Long](0)
    if (timeDiff <= sessionExpirationSeconds * 1000) {
      buffer(2) = buffer.getAs[Map[Timestamp, String]](2) + (input.getAs[Timestamp](0) -> currentSessionId.toString)
    } else {
      buffer(2) = buffer.getAs[Map[Timestamp, String]](2) + (input.getAs[Timestamp](0) -> (currentSessionId + 1).toString)
      buffer(0) = currentSessionId + 1
    }

    buffer(1) = input.getAs[Timestamp](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val timeDiff = if (buffer1.getAs[Timestamp](1).getTime == 0) {
      0
    } else {
      buffer2.getAs[Timestamp](1).getTime - buffer1.getAs[Timestamp](1).getTime
    }

    val currentSessionId = buffer1.getAs[Long](0)
    if (timeDiff <= sessionExpirationSeconds * 1000) {
      buffer1(2) = buffer1.getAs[Map[Timestamp, String]](2) + (buffer2.getAs[Timestamp](1) -> currentSessionId.toString)
    } else {
      buffer1(2) = buffer1.getAs[Map[Timestamp, String]](2) + (buffer2.getAs[Timestamp](1) -> (currentSessionId + 1).toString)
      buffer1(0) = currentSessionId + 1
    }

    buffer1(1) = buffer2.getAs[Timestamp](1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Map[Timestamp, String]](2)
  }
}