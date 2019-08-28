package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.common.DateUtils
import it.unibo.bd1819.utils.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

class Job2Main {

  def executeJob(sc: SparkContext, conf: Configuration, sqlc: SQLContext): Unit = {
    val sqlContext = sqlc
    import sqlc.implicits._
    val questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
    val questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
    val scoreAnswersDF = sqlContext.sql("select Id, Score, AnswersCount from questions")
    val joinDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
    joinDF.createOrReplaceTempView("joinDF")
    val binDF = sqlContext.sql("select tag, Score, AnswerCount from joinDF")
      .map(row => (row.getString(0), 
        Bin.getBinFor(Integer.parseInt(row.getString(1)), Bin.DEFAULT_SCORE_THRESHOLD, 
          Integer.parseInt(row.getString(2)), Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD)).toString())
      .withColumnRenamed("_1", "tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    binDF.show()
    val binCountDF = sqlContext.sql("select tag, Bin, count(*) as Count from binDF group by tag, Bin")
    binCountDF.show()
  }
}

object Job2Main{
  def apply: Job2Main = new Job2Main()
}
