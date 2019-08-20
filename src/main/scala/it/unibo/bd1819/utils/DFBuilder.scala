package it.unibo.bd1819.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

/**
  * An objext to build all the needed dataframe.
  */
object DFBuilder {

  val QUESTION_TAGS_TABLE_NAME = "question_tags"
  val QUESTIONS_TABLE_NAME = "questions"

  /**
    * Build the Questions Dataframe and save the temp table.
    * @param sparkContext the specific spark context
    * @param sqlContext the sql contex to interrogate
    * @param tableName the name of the table to set.
    * @return a DF linked to the questions data
    */
    def getQuestionsDF(sparkContext: SparkContext, sqlContext: SQLContext, tableName:String = QUESTIONS_TABLE_NAME) = {
      val questionsCsv = sparkContext.textFile(PathVariables.QUESTIONS_PATH /*, 8*/)
      val questionsSchema = questionsCsv.first()
      val questionsSchemaType = FileParsing.StringToSchema(questionsSchema, FileParsing.FIELD_SEPARATOR)
      val questionsSchemaRDD = questionsCsv.map(_.split(FileParsing.FIELD_SEPARATOR))
        .map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6)))
      val questionsDF = sqlContext.createDataFrame(questionsSchemaRDD, questionsSchemaType)
      questionsDF.createOrReplaceTempView(tableName)
      questionsDF.cache()
      questionsDF
    }

}
