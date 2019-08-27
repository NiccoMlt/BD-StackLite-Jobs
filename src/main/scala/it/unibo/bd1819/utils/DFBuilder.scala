package it.unibo.bd1819.utils

import it.unibo.bd1819.common.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * An object to build all the needed DataFrame.
  */
object DFBuilder {

  val QUESTION_TAGS_TABLE_NAME = "question_tags"
  val QUESTIONS_TABLE_NAME = "questions"

  private def getSchemaFromFile(file: RDD[String]): String = file.first()
  
  /**
    * Build the Questions DataFrame and save the temp table.
    * @param sqlContext the sql context to query
    * @return a DF linked to the questions data
    */
    def getQuestionsDF(sqlContext: SQLContext, isTags: Boolean): DataFrame = {
      val questionsDF =  sqlContext.read.format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("inferSchema", "true")
        .load(
        if(isTags) PathVariables.QUESTION_TAGS_PATH else PathVariables.QUESTIONS_PATH
      )
      questionsDF.createOrReplaceTempView( if(isTags) QUESTION_TAGS_TABLE_NAME else QUESTIONS_TABLE_NAME)
      questionsDF.cache()
      questionsDF
     
    }
  
}
