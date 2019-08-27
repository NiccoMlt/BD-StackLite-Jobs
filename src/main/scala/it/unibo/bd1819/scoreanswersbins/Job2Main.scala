package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.common.DateUtils
import it.unibo.bd1819.utils.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

class Job2Main {

  def executeJob(conf: Configuration, sqlc: SQLContext): Unit = {
    val sqlContext = sqlc

    val questionsDF = getQuestionsDF(sqlContext, isTags = false)
    val questionTagsDF = getQuestionsDF(sqlContext, isTags = true)
  }
}

object Job2Main{
  def apply: Job2Main = new Job2Main()
}
