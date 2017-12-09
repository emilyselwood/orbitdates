package org.wselwood.orbitdates

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions._

object BlogTest {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]") // if you have more cores or a cluster turn this up.
      .appName("Spark SQL join observations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/")

    val mpcobs = args(0) // The json data file of objects
    val observationPath = args(1)

    val orbRec = spark.read
      .option("multiLine", true)
      .json(mpcobs)
      .withColumn("number_formatted", new Column(Substring(col("Number").expr, lit(2).expr, (length(col("Number"))-2).expr)))
      .withColumn("id", coalesce(col("number_formatted"), col("Principal_desig")))
      .withColumn("year_firstObs", new Column(Substring(col("Arc_years").expr, lit(0).expr, lit(4).expr)))
      .checkpoint()


    val dateConvert = udf(ObsUtils.dateFunc _)
    val unpackId = udf(ObsUtils.unpackIdFunc _)
    val formatDate = udf(ObsUtils.formatDateFunc _)

    val obs = spark.read.text(observationPath)
      .withColumn("id", unpackId(substring(col("value"), 6, 7)))
      .withColumn("ts", dateConvert(substring(col("value"), 16, 16)))
      .groupBy(col("id"))
      .min("ts")
      .withColumn("date", formatDate(col("min(ts)")))
      .select("id", "date")
      .checkpoint()

    val joined = orbRec.join(obs, Seq("id", "id"))
      .select(
        col("id"),
        col("Name"),
        col("date"),
        col("a"),
        col("e"),
        col("i"),
        col("Epoch"),
        col("H"),
        col("G"),
        col("Node"),
        col("Peri")
      )


    joined.write.mode(SaveMode.Overwrite).csv("out.csv")

  }

}
