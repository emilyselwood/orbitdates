package org.wselwood.orbitdates

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions._

object SplitByYear {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]") // if you have more cores or a cluster turn this up.
      .appName("Spark SQL join observations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/")

    val dateConvert = udf(ObsUtils.dateFunc _)
    val formatDate = udf(ObsUtils.yearFunc _)

    val obs = spark.read.text(args(0))
      .withColumn("year", formatDate(dateConvert(substring(col("value"), 16, 16))))
      .withColumn("key", when(col("year").lt(2000), 1).otherwise(col("year")))


    obs.checkpoint(true)

    obs.where(col("key") === 1).select(col("value")).write.mode(SaveMode.Overwrite).csv(s"out.csv")
    (2000 to 2018).foreach( y => {

      obs.where(col("key") === y).select(col("value")).write.mode(SaveMode.Overwrite).csv(s"out${y}.csv")
    })


  }

}
