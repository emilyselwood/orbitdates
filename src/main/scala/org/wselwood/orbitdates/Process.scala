package org.wselwood.orbitdates

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions._


object Process {

  def main (args : Array[String]) : Unit = {
    if ( args.length < 2 ) {
      println("Requires path to observation file and mpcobs file")
      return
    }

    val observationPath = args(0)
    val mpcobs = args(1)

    val spark = SparkSession
      .builder()
      .master("local[2]") // if you have more cores or a cluster turn this up.
      .appName("Spark SQL join observations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/")

    val dateConvert = udf(dateFunc _)
    val unpackId = udf(unpackIdFunc _)
    val formatDate = udf(formatDateFunc _)

    val obs = spark.read.text(observationPath)
      .withColumn("id", unpackId(substring(col("value"), 0, 5)))
      .withColumn("ts", dateConvert(substring(col("value"), 16, 16)))
      .groupBy(col("id"))
      .min("ts")
      .checkpoint()

    val orbRec = spark.read
      .option("multiLine", true)
      .json(mpcobs)
      .withColumn("id", new Column(Substring(col("Number").expr, lit(2).expr, (length(col("Number"))-2).expr)))
      .checkpoint()

    val result = obs.join(orbRec, "id")
      .select(
        col("id"),
        col("Name"),
        formatDate(col("min(ts)")).as("FirstObs"),
        col("a"),
        col("e"),
        col("i"),
        col("Epoch"),
        col("H"),
        col("G"),
        col("Node"),
        col("Peri")
      )
      .checkpoint()

    //id,Name,FirstObs,a,e,i,Epoch,H,G,Node,Peri

    result.write.mode(SaveMode.Overwrite).csv("out.csv")

    println("obs objects: " + obs.count() + " objects: " + orbRec.count() + " result: " + result.count())

  }

  private def dateFunc(in : String): Long = {
    val year = in.substring(0, 4).toInt
    val month = in.substring(5, 7).toInt
    val day = in.substring(8, 10).toInt

    val part = in.substring(11).replaceAll(" ", "0").toInt
    val seconds = Math.round(((24*60*60) * 0.00001) * part)

    LocalDate.of(year, month, day).atStartOfDay()
      .plus(seconds, ChronoUnit.SECONDS)
      .toInstant(ZoneOffset.UTC)
      .getEpochSecond
  }

  private def trimZeroFunc(in: String) : String = {
    var i = 0
    while(in(i) == '0') {
      i = i + 1
    }
    in.substring(i)
  }

  private def unpackIdFunc(in : String) : String = {
    if (in(0) >= '0' && in(0) <= '9') {
      trimZeroFunc(in)
    } else {
      val numeric = in.substring(1).toInt
      val expanded = if (in(0) >= 'a' && in(0) <= 'z') {
        in(0) - 'a' + 36
      } else {
        in(0) - 'A' + 10
      }

      expanded.toString + numeric.toString
    }
  }

  private def formatDateFunc(in : Long) : String = {
    LocalDateTime.ofEpochSecond(in, 0, ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }

}
