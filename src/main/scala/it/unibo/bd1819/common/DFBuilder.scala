package it.unibo.bd1819.common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * An object to build all the needed DataFrame.
 */
object DFBuilder {

  val QUESTION_TAGS_TABLE_NAME = "question_tags"
  val QUESTIONS_TABLE_NAME = "questions"

  /**
   * Resolve an RDD and its schema for CSV at given path.
   *
   * @param path the path to the CSV file with schema in its first line
   * @param sc   the Spark Context
   * @return an RDD with the single elements of each row and the schema from the CSV 
   */
  def getRddFor(path: String, sc: SparkContext): (RDD[String], StructType) = {
    val tmpCsv: RDD[String] = sc.textFile(path)
    val schema: String = getSchemaFromFile(tmpCsv)
    val csv: RDD[String] = tmpCsv.filter(row => row != schema)
    val schemaType: StructType = FileParsing.StringToSchema(schema, FileParsing.FIELD_SEPARATOR)
    (csv, schemaType)
  }

  /**
   * Build the Question Tags DataFrame and save the temp table.
   *
   * @param sc         the Spark Context
   * @param sqlContext the SQL Context to query
   * @return a DF linked to the questions data
   */
  def getQuestionTagsDF(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val (questionsCsv: RDD[String], questionsSchemaType: StructType) = getRddFor(PathVariables.QUESTION_TAGS_PATH, sc)
    val questionsSchemaRDD: RDD[Row] = questionsCsv
      .map(row => row.split(FileParsing.FIELD_SEPARATOR))
      .map(e => Row(e(0), e(1)))
    val questionsDF: DataFrame = sqlContext.createDataFrame(questionsSchemaRDD, questionsSchemaType)
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
    val (questionsCsv: RDD[String], questionsSchemaType: StructType) = getRddFor(PathVariables.QUESTIONS_PATH, sc)
    val questionsSchemaRDD = questionsCsv
      .map(row => row.split(FileParsing.FIELD_SEPARATOR))
      .map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6)))
    val questionsDF = sqlContext.createDataFrame(questionsSchemaRDD, questionsSchemaType)
    questionsDF.createOrReplaceTempView(QUESTIONS_TABLE_NAME)
    questionsDF
  }

  /**
   * Resolve the schema from the first line of the file.
   *
   * @param file the file
   * @return the schema
   */
  private def getSchemaFromFile(file: RDD[String]): String = file.first()
}
