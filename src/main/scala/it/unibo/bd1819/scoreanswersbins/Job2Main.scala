package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.common.{Configuration, JobMainAbstract}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job2Main extends JobMainAbstract{

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and Score and AnswerCount columns from the questions DF */
    val scoreAnswersDF = sqlContext.sql("select Id, Score, AnswerCount from questions")
    
    /* Join the previously obtained DF to the question_tags DF, dropping the useless column containing the Ids.
     * Select then all columns from the resulting DF, and map the Score and AnswerCount columns into one Bin
     * column that will have data representing in which Bin
     * (Between HIGH_SCORE_HIGH_COUNT, HIGH_SCORE_LOW_COUNT, LOW_SCORE_HIGH_COUNT, LOW_SCORE_LOW_COUNT) the pair
     * (Score, AnswerCount) must be located.
     * The resulting DF will only have two columns: tag and Bin.
     */
    val binDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
      .select("tag", "Score", "AnswerCount")
      .map(row => (row.getString(0),
      Bin.getBinFor(Integer.parseInt(if(row.getString(1) == "null" || row.getString(1) == "NA") "0" else row.getString(1)),
        Bin.DEFAULT_SCORE_THRESHOLD,
        Integer.parseInt(if(row.getString(2) == "null" || row.getString(2) == "NA") "0" else row.getString(2)),
        Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD).toString))
      .withColumnRenamed("_1", "Tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    binDF.cache()
    
    /* Add to the previous DF a column representing the amount of the occurrences of (Tag, Bin)
     * are into the DF itself.
     */
    val binCountDF = sqlContext.sql("select Tag, Bin, count(*) as Count from binDF group by Tag, Bin")
    binCountDF.createOrReplaceTempView("binCountDF")
    binCountDF.cache()
    
    /* Generate a DF that shows a column with the four bins, and, for each one of them, a list of couples (Tag - Count) */
    val finalDF = sqlContext.sql("select Bin, collect_list(distinct concat(Tag,' - ',Count)) as ListTagCount " +
      "from binCountDF group by Bin")
    finalDF.show()
    
  }
}

object Job2Main{
  def apply: Job2Main = new Job2Main()
}
