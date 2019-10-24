package com.bastrich

import com.bastrich.utils.Utils.verifyScheme
import org.apache.spark.sql.DataFrame

class Task2_3 {

  def findCategoryProductsTop(df: DataFrame): DataFrame = {
    if (!verifyScheme(df.schema)) {
      throw new Exception("Wrong input data scheme")
    }

    df.createOrReplaceTempView("events")

    df.sqlContext.sql(
      """
        |WITH maxProductTimeSpendings AS (SELECT category,
        |                                        product,
        |                                        max(productSpentTimeSeconds) as productSpentTimeSeconds
        |                                 FROM (SELECT DISTINCT category,
        |                                                       product,
        |                                                       userId,
        |                                                       unix_timestamp(max(eventTime) OVER (PARTITION BY category, product, userId ORDER BY eventTime)) -
        |                                                           unix_timestamp(min(eventTime) OVER (PARTITION BY category, product, userId ORDER BY eventTime)) as productSpentTimeSeconds
        |                                       FROM events)
        |                                 GROUP BY category, product
        |                                 ORDER BY productSpentTimeSeconds DESC)
        |
        |SELECT category,
        |       product,
        |       dense_rank() OVER (PARTITION BY category ORDER BY productSpentTimeSeconds desc) as rank
        |FROM maxProductTimeSpendings
        |""".stripMargin
    )
  }
}