package org.wselwood.orbitdates

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions._


object DateOfFirstObs {

  def main (args : Array[String]) : Unit = {
    if ( args.length < 3 ) {
      println("Requires path to observation file and mpcobs file and a boolean value of if it is a numbered file")
      return
    }

    val observationPath = args(0)
    val mpcobs = args(1)
    val isNumbered = args(2) == "true"

    val spark = SparkSession
      .builder()
      .master("local[2]") // if you have more cores or a cluster turn this up.
      .appName("Spark SQL join observations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/")

    val dateConvert = udf(ObsUtils.dateFunc _)
    val unpackId = udf(ObsUtils.unpackIdFunc _)
    val formatDate = udf(ObsUtils.formatDateFunc _)

    val (idStart, idLen) = if (isNumbered) {
      (0, 5)
    } else {
      (6, 7)
    }

    val obs = spark.read.text(observationPath)
      .withColumn("id", unpackId(substring(col("value"), idStart, idLen)))
      .withColumn("ts", dateConvert(substring(col("value"), 16, 16)))
      .groupBy(col("id"))
      .min("ts")
      .checkpoint()

    //obs.show(10)

    val orbRec = spark.read
      .option("multiLine", true)
      .json(mpcobs)
      .withColumn("number_formatted", new Column(Substring(col("Number").expr, lit(2).expr, (length(col("Number"))-2).expr)))
      .withColumn("id", coalesce(col("number_formatted"), col("Principal_desig")))
      .checkpoint()


    //orbRec.show(10)

    val result = obs.join(orbRec, obs.col("id").equalTo(orbRec.col("id")), "inner")
      .select(
        obs.col("id"),
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

    //result.where("a is null").show()

    //id,Name,FirstObs,a,e,i,Epoch,H,G,Node,Peri

    result.write.mode(SaveMode.Overwrite).csv("out.csv")

    println("obs objects: " + obs.count() + " objects: " + orbRec.count() + " result: " + result.count())

  }


}
