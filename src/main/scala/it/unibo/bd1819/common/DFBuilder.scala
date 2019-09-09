package it.unibo.bd1819.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * An object to build all the needed DataFrame.
 */
object DFBuilder {

  val QUESTION_TAGS_TABLE_NAME = "question_tags"
  val QUESTIONS_TABLE_NAME = "questions"
  
  def getDF(path: String, sqlCont: SQLContext): DataFrame = {
    sqlCont.read.option("header", "true").option("inferSchema", "false").csv(path)
  }

  /**
   * Build the Question Tags DataFrame and save the temp table.
   *
   * @param sc         the Spark Context
   * @param sqlContext the SQL Context to query
   * @return a DF linked to the questions data
   */
  def getQuestionTagsDF(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val questionsDF: DataFrame = getDF(PathVariables.QUESTION_TAGS_PATH, sqlContext)
    questionsDF.createOrReplaceTempView(QUESTION_TAGS_TABLE_NAME)
    questionsDF
  }

  /**
   * Build the Questions DataFrame and save the temp table.
   *
   * @param sc         the Spark Context
   * @param sqlContext the SQL Context to query
   * @return a DF linked to the questions data
   */
  def getQuestionsDF(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val questionsDF = getDF(PathVariables.QUESTIONS_PATH, sqlContext)
    questionsDF.createOrReplaceTempView(QUESTIONS_TABLE_NAME)
    questionsDF
  }
}
