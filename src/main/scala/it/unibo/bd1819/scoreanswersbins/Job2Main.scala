package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.common.JobMainAbstract
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job2Main extends JobMainAbstract {

  def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and Score and AnswerCount columns from the questions DF */
    val scoreAnswersDF = sqlCont.sql("select Id, Score, AnswerCount from questions")
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
      .filter($"Score" notEqual "NA")
      .filter($"AnswerCount" notEqual "NA")
      .map(row => (row.getString(0),
        Bin.getBinFor(Integer.parseInt(row.getString(1)), Job2Main.IMPROVED_SCORE_THRESHOLD,
          Integer.parseInt(row.getString(2)), Job2Main.IMPROVED_ANSWER_THRESHOLD).toString))
      .withColumnRenamed("_1", "Tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    
    /* Add to the previous DF a column representing the amount of the occurrences of (Tag, Bin)
     * are into the DF itself.
     */
    val binCountDF = sqlCont.sql("select Tag, Bin, count(*) as Count from binDF group by Tag, Bin")
    binCountDF.createOrReplaceTempView("binCountDF")
    
    /* Generate a DF that shows a column with the four bins, and, for each one of them, a list of couples (Tag - Count) */
    val finalDF = sqlCont.sql("select Bin, collect_list(distinct concat(Tag,' - ',Count)) as ListTagCount " +
      "from binCountDF group by Bin")

    /* Save DF as Table on our Hive DB */
    finalDF.write./*mode(SaveMode.Overwrite).*/saveAsTable(job2FinalTableName)
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
  
  /*
  * Circa 1/3 delle domande hanno 0 risposte.
  * Circa 2/5 delle domande hanno meno di 4 come score.
  */
  
  def apply(): Job2Main = new Job2Main()
}
