package it.unibo.bd1819.common

import DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class JobMainAbstract {
  var sqlContext: SQLContext = _
  var questionsDF: DataFrame = _
  var questionTagsDF: DataFrame = _
  val job1FinalTableName: String = PathVariables.JOB_TABLE_NAME + "1"
  val job2FinalTableName: String = PathVariables.JOB_TABLE_NAME + "2"

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit

  protected def configureEnvironment(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    // If user has not specified partitions and tasks for each partitions jobs use default
    // TODO: why shouldn't I use the passed sqlCont ???
    if (conf.partitions == 0) {
      sqlContext = JobConfigurator.getDefault(sqlCont).getSetSqlContext
    } else {
      sqlContext = JobConfigurator(sqlCont, conf).getSetSqlContext
    }
    
    sqlContext.sql("drop table if exists " + job1FinalTableName)
    sqlContext.sql("drop table if exists " + job2FinalTableName)
    
    this.questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
    this.questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
  }
}
