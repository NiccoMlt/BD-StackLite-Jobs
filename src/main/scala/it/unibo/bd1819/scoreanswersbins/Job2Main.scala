package it.unibo.bd1819.scoreanswersbins

import java.sql.Struct

import it.unibo.bd1819.common.JobMainAbstract
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

class Job2Main extends JobMainAbstract {

  def limitSize(n: Int, arrCol: String): String = Array( (0 until n).map( arrCol ): _* ).mkString(" ")
  
  def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and Score and AnswerCount columns from the questions DF */
    val scoreAnswersDF = sqlCont.sql("select Id, Score, AnswerCount from questions")
      .filter($"Score" notEqual "NA")
      .filter($"AnswerCount" notEqual "NA")
    scoreAnswersDF.cache()
    
    /* Join the previously obtained DF to the question_tags DF, dropping the useless column containing the Ids.
     * Select then all columns from the resulting DF, filter "NA" values that will affect the analysis
     * and finally map the Score and AnswerCount columns into one Bin
     * column that will have data representing in which Bin
     * (Between HIGH_SCORE_HIGH_COUNT, HIGH_SCORE_LOW_COUNT, LOW_SCORE_HIGH_COUNT, LOW_SCORE_LOW_COUNT) the pair
     * (Score, AnswerCount) must be located.
     * The resulting DF will only have two columns: tag and Bin.
     */
    val binDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
      .select("tag", "Score", "AnswerCount")
      .map(row => (row.getString(0),
        Bin.getBinFor(Integer.parseInt(row.getString(1)), Job2Main.IMPROVED_SCORE_THRESHOLD,
          Integer.parseInt(row.getString(2)), Job2Main.IMPROVED_ANSWER_THRESHOLD).toString))
      .withColumnRenamed("_1", "Tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    
    /* Add to the previous DF a column representing the amount of the occurrences of (Tag, Bin)
     * are into the DF itself. And order the table by the count and Bin.
     */
    val binCountDF = sqlCont.sql("select Bin, Tag, count(*) as Count from binDF group by Bin, Tag")
    binCountDF.createOrReplaceTempView("binCountDF")
    
    /* Generate a DF that shows a column with the four bins, and, for each one of them, a list of couples (Tag - Count) */
    val finalDF = sqlCont.sql("select Bin, collect_list(struct(Tag, Count)) as ListTagCount " +
      "from binCountDF group by Bin")
    /* Use the RDD to make operation over the List of couples: I want to order by Count and take the first 10 of them */
    val content = finalDF.rdd
      .map(row => (row.getString(0), row.getSeq[Row](1)))
      .map(r => (r._1, r._2.map( row => (
        row.getAs[String]("Tag"),
        row.getAs[Long]("Count")
      ))))
      .map(row => (row._1, row._2
        .sortBy(x => x._2)(Ordering.Long.reverse)
        .take(Job2Main.topNumberOfTagsForEachBin)
        .mkString(" , ")))
    val finalSchema = new StructType().add("Bin", StringType).add("TagCount", StringType)
    val finalContent = content.map(keyValueRow => Row(keyValueRow._1, keyValueRow._2))
    val hopeDF = sqlCont.createDataFrame(finalContent, finalSchema)
    hopeDF.write.mode(SaveMode.Overwrite).saveAsTable(job2FinalTableName)
  }
  
  override protected def dropTables(sqlCont: SQLContext): Unit = {
    sqlCont.sql("drop table if exists " + job2FinalTableName)
  }
}

object Job2Main {

  /**
   * Improved thresholds for Score and AnswerCount, modified due to multiple trials.
   * These values should balance enough the dataset into the four bins.
   */
  private val IMPROVED_SCORE_THRESHOLD = Bin.DEFAULT_SCORE_THRESHOLD
  private val IMPROVED_ANSWER_THRESHOLD = Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD
  
  private val topNumberOfTagsForEachBin = 10
  /*
  * Circa 1/3 delle domande hanno 0 risposte.
  * Circa 2/5 delle domande hanno meno di 4 come score.
  */
  
  def apply(): Job2Main = new Job2Main()
}
