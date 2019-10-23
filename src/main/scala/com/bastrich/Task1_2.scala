package com.bastrich

import java.sql.Timestamp

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class Task1_2 {
  def enrichWithSessionIds(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("events")

    def findSession = (sessions: Map[Timestamp, String], eventTime: Timestamp) => {
      sessions.get(eventTime)
    }


    df.sparkSession.udf.register("sessions", new Sessions)
    df.sparkSession.udf.register("findSession", findSession)

    df.sqlContext.sql(
      """
        |WITH sessionIds as (select events.category,
        |                           events.product,
        |                           events.userId,
        |                           events.eventTime,
        |                           events.eventType,
        |                           findSession(sessionAggs.sessions, events.eventTime) as sessionId
        |                    from events
        |                             join
        |                         (select category,
        |                                 userId,
        |                                 sessions(eventTime) as sessions
        |                          from (select * from events order by eventTime)
        |                          group by category, userId) sessionAggs
        |                         ON events.category == sessionAggs.category and events.userId == sessionAggs.userId)
        |
        |select sessionIds.category,
        |       sessionIds.product,
        |       sessionIds.userId,
        |       sessionIds.eventTime,
        |       sessionIds.eventType,
        |       concat(sessionIds.sessionId, '-', sessionIds.userId, '-', sessionIds.category) as sessionId,
        |       sessionStartTime,
        |       sessionEndTime
        |from sessionIds
        |         join
        |     (select category, userId, sessionId, min(eventTime) as sessionStartTime, max(eventTime) as sessionEndTime
        |      from sessionIds
        |      group by category, userId, sessionId) minMaxSessionTimes
        |     ON sessionIds.category = minMaxSessionTimes.category and sessionIds.userId = minMaxSessionTimes.userId and
        |        sessionIds.sessionId = minMaxSessionTimes.sessionId
        |""".stripMargin)
  }
}

class Sessions extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", TimestampType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("currentSessionId", LongType) ::
      StructField("pastTimestamp", TimestampType) ::
      StructField("sessions", MapType(TimestampType, StringType)) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = MapType(TimestampType, StringType)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = new Timestamp(0)
    buffer(2) = Map[Timestamp, String]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val timeDiff = if (buffer.getAs[Timestamp](1).getTime == 0) {
      0
    } else {
      input.getAs[Timestamp](0).getTime - buffer.getAs[Timestamp](1).getTime
    }

    val currentSessionId = buffer.getAs[Long](0)
    if (timeDiff <= 300 * 1000) {
      buffer(2) = buffer.getAs[Map[Timestamp, String]](2) + (input.getAs[Timestamp](0) -> currentSessionId.toString)
    } else {
      buffer(2) = buffer.getAs[Map[Timestamp, String]](2) + (input.getAs[Timestamp](0) -> (currentSessionId + 1).toString)
      buffer(0) = currentSessionId + 1
    }

    buffer(1) = input.getAs[Timestamp](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val timeDiff = if (buffer1.getAs[Timestamp](1).getTime == 0) {
      0
    } else {
      buffer2.getAs[Timestamp](1).getTime - buffer1.getAs[Timestamp](1).getTime
    }

    val currentSessionId = buffer1.getAs[Long](0)
    if (timeDiff <= 300 * 1000) {
      buffer1(2) = buffer1.getAs[Map[Timestamp, String]](2) + (buffer2.getAs[Timestamp](1) -> currentSessionId.toString)
    } else {
      buffer1(2) = buffer1.getAs[Map[Timestamp, String]](2) + (buffer2.getAs[Timestamp](1) -> (currentSessionId + 1).toString)
      buffer1(0) = currentSessionId + 1
    }

    buffer1(1) = buffer2.getAs[Timestamp](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Map[Timestamp, String]](2)
  }
}