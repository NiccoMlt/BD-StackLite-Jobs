package it.unibo.bd1819

import it.unibo.bd1819.common.Configuration
import it.unibo.bd1819.daysproportion.Job1Main
import it.unibo.bd1819.ml.JobMlMain
import it.unibo.bd1819.scoreanswersbins.Job2Main
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object ScalaMain {

  final private val JOB1 = "JOB1"
  final private val JOB2 = "JOB2"
  final private val JOBML = "JOBML"

  def main(args: Array[String]): Unit = {
    if (args.length != 1 && args.length != 4) {
      println("USAGE: ./bd-stacklite-jobs-1.0.0-spark.jar <JOB1 | JOB2>  [PARTITIONS PARALLELISM MEMORY]")
      println("Found: " + args.length)
    } else {
      val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("BD-StackLite-Job")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
      val sc: SparkContext = spark.sparkContext
      val sqlCtx: SQLContext = spark.sqlContext

      (args(0) match {
        case JOB1 => Job1Main()
        case JOB2 => Job2Main()
        case JOBML => JobMlMain()
      }).executeJob(
        sc,
        if (args.length == 4) Configuration(args.toList)
        else Configuration(),
        sqlCtx)
    }
  }
}
