package it.unibo.bd1819.common

import DFBuilder.{getQuestionTagsDF, getQuestionsDF}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class JobMainAbstract {
  var questionsDF: DataFrame = _
  var questionTagsDF: DataFrame = _
  val job1FinalTableName: String = PathVariables.JOB_TABLE_NAME + "1"
  val job2FinalTableName: String = PathVariables.JOB_TABLE_NAME + "2"

  def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit

  protected def configureEnvironment(sc: SparkContext, sqlCont: SQLContext): Unit = {
    sqlCont.sql("drop table if exists " + job1FinalTableName)
    sqlCont.sql("drop table if exists " + job2FinalTableName)
    
    this.questionsDF = getQuestionsDF(sc, sqlCont)
    this.questionTagsDF = getQuestionTagsDF(sc, sqlCont)
  }
}
