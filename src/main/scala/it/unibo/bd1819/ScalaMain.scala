package it.unibo.bd1819

import it.unibo.bd1819.common.Configuration
import it.unibo.bd1819.daysproportion.Job1Main
import it.unibo.bd1819.scoreanswersbins.Job2Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ScalaMain {

  final private val JOB1 = "JOB1"
  final private val JOB2 = "JOB2"

  def main(args: Array[String]): Unit = {
    if (args.length != 1 && args.length != 4) {
      println("USAGE: ./bd-stacklite-jobs-1.0.0-spark.jar <JOB1 | JOB2>  [PARTITIONS PARALLELISM MEMORY]")
      println("Found: " + args.length)
    } else {
      val conf: SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
      val sc: SparkContext = new SparkContext(conf)
      val sqlContext = SparkSession.builder.master("local[*]").getOrCreate.sqlContext
      if (args.length == 4) {
        val conf = Configuration(args.toList)
        if (args(0) == JOB1) {
          Job1Main.apply.executeJob(sc, conf, sqlContext)
        } else {
          Job2Main.apply.executeJob(sc, conf, sqlContext)
        }
      } else {
        if (args(0) == JOB1) {
          Job1Main.apply.executeJob(sc, Configuration(), sqlContext)
        } else {
          Job2Main.apply.executeJob(sc, Configuration(), sqlContext)
        }
      }
    }
  }
}
