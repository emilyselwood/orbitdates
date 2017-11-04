package org.wselwood.orbitdates

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring, udf}

/**
  * Number of objects discovered on each day of the week.
  */
object DateAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]") // if you have more cores or a cluster turn this up.
      .appName("Spark SQL join observations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/")

    val dateConvert = udf(ObsUtils.dateFunc _)
    val formatDate = udf(ObsUtils.dayOfWeekFunc _)

    val obs = spark.read.text(args(0))
      .withColumn("id", substring(col("value"), 0, 5))
      .withColumn("ts", dateConvert(substring(col("value"), 16, 16)))
      .groupBy(col("id"))
      .min("ts")
      .groupBy(formatDate(col("min(ts)")))
      .count()

    obs.describe("count")


  }

}
