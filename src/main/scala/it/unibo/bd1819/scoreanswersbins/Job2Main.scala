package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.common.{Configuration, JobMainAbstract}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job2Main extends JobMainAbstract{

  /**
   * Utility method used to filter non-relevant data, assigning replacing
   * values "null" and "NA" with a 0
   * @param stringToCheck the value of Score or AnswerCount, as a String
   * @return the filtered value, as a String
   */
  private def avoidNullAndNA(stringToCheck: String): String = 
    if(stringToCheck == "null" || stringToCheck == "NA") "0" else stringToCheck

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and Score and AnswerCount columns from the questions DF */
    val scoreAnswersDF = sqlContext.sql("select Id, Score, AnswerCount from questions")
    
    /* Join the previously obtained DF to the question_tags DF, dropping the useless column containing the Ids. */
    val joinDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
    joinDF.createOrReplaceTempView("joinDF")

    /* Select then all columns from the resulting DF, and map the Score and AnswerCount columns into one Bin
     * column that will have data representing in which Bin
     * (Between HIGH_SCORE_HIGH_COUNT, HIGH_SCORE_LOW_COUNT, LOW_SCORE_HIGH_COUNT, LOW_SCORE_LOW_COUNT) the pair
     * (Score, AnswerCount) must be located.
     * The resulting DF will only have two columns: tag and Bin.
     */
    val binDF = sqlContext.sql("select tag, Score, AnswerCount from joinDF")
      .map(row => (row.getString(0),
        Bin.getBinFor(Integer.parseInt(avoidNullAndNA(row.getString(1))),
          Bin.DEFAULT_SCORE_THRESHOLD,
          Integer.parseInt(avoidNullAndNA(row.getString(2))),
          Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD).toString))
      .withColumnRenamed("_1", "Tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    
    /* Add to the previous DF a column representing the amount of the occurrences of (Tag, Bin)
     * are into the DF itself.
     */
    val binCountDF = sqlContext.sql("select Tag, Bin, count(*) as Count from binDF group by Tag, Bin")
    binCountDF.createOrReplaceTempView("binCountDF")
    
    /* Generate a DF that shows a column with the four bins, and, for each one of them, a list of couples (Tag - Count) */
    val finalDF = sqlContext.sql("select Bin, collect_list(distinct concat(Tag,' - ',Count)) as ListTagCount " +
      "from binCountDF group by Bin")
    finalDF.show()
    
  }
}

object Job2Main{
  def apply: Job2Main = new Job2Main()
}
