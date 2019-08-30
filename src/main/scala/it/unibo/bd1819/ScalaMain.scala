package it.unibo.bd1819

import it.unibo.bd1819.common.Configuration
import it.unibo.bd1819.daysproportion.Job1Main
import it.unibo.bd1819.scoreanswersbins.Job2Main
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaMain {

  final private val JOB1 = "JOB1"
  final private val JOB2 = "JOB2"
  
  def main(args: Array[String]): Unit = {
    if (args.length != 1 && args.length != 4) {
      println("USAGE: ./bd-stacklite-jobs-1.0.0-spark.jar <JOB1 | JOB2>  [PARTITIONS PARALLELISM MEMORY]")
      println("Found: " + args.length)
    } else {
      val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("BD-StackLite-Job")
        .getOrCreate()
      val sc: SparkContext = spark.sparkContext
      val sqlCtx: SQLContext = spark.sqlContext
      
      if (args.length == 4) {
        val conf = Configuration(args.toList)
        if (args(0) == JOB1) {
          Job1Main.apply.executeJob(sc, conf, sqlCtx)
        } else {
          Job2Main.apply.executeJob(sc, conf, sqlCtx)
        }
      } else {
        if (args(0) == JOB1) {
          Job1Main.apply.executeJob(sc, Configuration(), sqlCtx)
        } else {
          Job2Main.apply.executeJob(sc, Configuration(), sqlCtx)
        }
      }
    }
  }
}
