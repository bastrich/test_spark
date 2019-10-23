package com.bastrich

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class Task2_3 {

  def findCategoryProductsTop(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      """
        |select category,
        |       product,
        |       dense_rank() OVER (PARTITION BY category ORDER BY productSpentTimeSeconds desc) as rank
        |from (select category,
        |             product,
        |             max(productSpentTimeSeconds) as productSpentTimeSeconds
        |      from (select distinct category,
        |                            product,
        |                            userId,
        |                            unix_timestamp(max(eventTime)
        |                                           OVER (PARTITION BY category, product, userId ORDER BY eventTime)) -
        |                            unix_timestamp(min(eventTime)
        |                                           OVER (PARTITION BY category, product, userId ORDER BY eventTime)) as productSpentTimeSeconds
        |
        |            from events)
        |      group by category, product
        |      order by productSpentTimeSeconds desc)
        |
        |""".stripMargin
    )
  }
}