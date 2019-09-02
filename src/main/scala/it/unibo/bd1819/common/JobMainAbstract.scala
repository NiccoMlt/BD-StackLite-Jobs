package it.unibo.bd1819.common

import it.unibo.bd1819.common.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class JobMainAbstract {
  var sqlContext: SQLContext = _
  var questionsDF: DataFrame = _
  var questionTagsDF: DataFrame = _

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit

  protected def configureEnvironment(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    // If users has not specified partitions and tasks for each partitions jobs use default
    // TODO: why shouldn't I use the passed sqlCont ???
    if (conf.partitions == 0) {
      sqlContext = JobConfigurator.getDefault(sqlCont).getSetSqlContext
    } else {
      sqlContext = JobConfigurator(sqlCont, conf).getSetSqlContext
    }
    this.questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
    this.questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
  }
}
