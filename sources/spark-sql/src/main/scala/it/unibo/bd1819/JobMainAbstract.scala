package it.unibo.bd1819

import it.unibo.bd1819.scoreanswersbins.Configuration
import it.unibo.bd1819.utils.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class JobMainAbstract {
  var sqlContext : SQLContext = _
  var questionsDF: DataFrame = _
  var questionTagsDF: DataFrame = _
  protected def configureEnvironment(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    // If users has not specified partitions and tasks for each partitions jobs use default
    if (conf.partitions == 0) {
      sqlContext = JobConfigurator.getDefault(sqlCont).getSetSqlContext
    } else {
      sqlContext = JobConfigurator(sqlCont, conf).getSetSqlContext
    }
    this.questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
    this.questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
  }
  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit
}
