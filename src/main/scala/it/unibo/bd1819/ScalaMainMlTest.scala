package it.unibo.bd1819

import it.unibo.bd1819.common.DFBuilder.QUESTIONS_TABLE_NAME
import it.unibo.bd1819.common.{FileParsing, PathVariables}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

object ScalaMainMlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("BD-StackLite-Job")
      // .config("spark.default.parallelism", "8")
      // .config("spark.sql.shuffle.partitions", "8")
      // .config("spark.executor.memory", "5g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val sqlCtx: SQLContext = spark.sqlContext

    import sqlCtx.implicits._

    val tmpQuestionsCsv = sc.textFile(PathVariables.QUESTIONS_PATH).sample(withReplacement = false, 0.001)

    val questionsSchema = tmpQuestionsCsv.first()
    val questionsCsv = tmpQuestionsCsv.filter(row => row != questionsSchema)

    val questionsSchemaType = FileParsing.StringToSchema(questionsSchema, FileParsing.FIELD_SEPARATOR)
    val questionsSchemaRDD = questionsCsv.map(_.split(FileParsing.FIELD_SEPARATOR)).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6)))
    val questionsDF = sqlCtx.createDataFrame(questionsSchemaRDD, questionsSchemaType)
    questionsDF.createOrReplaceTempView(QUESTIONS_TABLE_NAME)
    val data = questionsDF.drop("Id", "CreationDate", "ClosedDate", "OwnerUserId")
      .filter($"DeletionDate".contains("NA"))
      .drop("DeletionDate")
      .filter((!$"AnswerCount".contains("NA")).and(!$"AnswerCount".contains("-")))

    val score: RDD[Double] = data.select("Score").map(row => row.getString(0).toDouble).rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val count: RDD[Double] = data.select("AnswerCount").map(row => row.getString(0).toDouble).rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val scCorrelationP: Double = Statistics.corr(score, count, "pearson")
    val scCorrelationS: Double = Statistics.corr(score, count, "spearman")

    println(s"Pearson Score/Count correlation: $scCorrelationP")
    println(s"Spearman Score/Count correlation: $scCorrelationS")
  }
}
