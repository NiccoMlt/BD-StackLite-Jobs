package it.unibo.bd1819.common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * An object to build all the needed DataFrame.
  */
object DFBuilder {

  val QUESTION_TAGS_TABLE_NAME = "question_tags"
  val QUESTIONS_TABLE_NAME = "questions"

  /**
   * Build the Questions DataFrame and save the temp table.
   *
   * @param sqlContext the sql context to query
   * @return a DF linked to the questions data
   */
  def getQuestionsDF(sc: SparkContext, sqlContext: SQLContext, isTags: Boolean): DataFrame = {
    val tmpQuestionsCsv = sc.textFile(
      if (isTags) PathVariables.QUESTION_TAGS_PATH else PathVariables.QUESTIONS_PATH /*, 8*/
    )
    val questionsSchema = getSchemaFromFile(tmpQuestionsCsv)
    val questionsCsv = tmpQuestionsCsv.filter(row => row != questionsSchema)
    val questionsSchemaType = FileParsing.StringToSchema(questionsSchema, FileParsing.FIELD_SEPARATOR)
    val questionsSchemaRDD = questionsCsv.map(_.split(FileParsing.FIELD_SEPARATOR))
      .map(e => if (isTags) Row(e(0), e(1)) else Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6)))
    val questionsDF = sqlContext.createDataFrame(questionsSchemaRDD, questionsSchemaType)
    questionsDF.createOrReplaceTempView(if (isTags) QUESTION_TAGS_TABLE_NAME else QUESTIONS_TABLE_NAME)
    questionsDF
  }

  private def getSchemaFromFile(file: RDD[String]): String = file.first()
}
