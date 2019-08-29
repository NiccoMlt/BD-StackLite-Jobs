package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.JobMainAbstract
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job2Main extends JobMainAbstract{
  
  private def avoidNullAndNA(stringToCheck: String): String = 
    if(stringToCheck == "null" || stringToCheck == "NA") "0" else stringToCheck

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._
    val scoreAnswersDF = sqlContext.sql("select Id, Score, AnswerCount from questions")
    val joinDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
    joinDF.createOrReplaceTempView("joinDF")
    joinDF.cache()
    val binDF = sqlContext.sql("select tag, Score, AnswerCount from joinDF")
      .map(row => (row.getString(0),
        Bin.getBinFor(Integer.parseInt(avoidNullAndNA(row.getString(1))),
          Bin.DEFAULT_SCORE_THRESHOLD,
          Integer.parseInt(avoidNullAndNA(row.getString(2))),
          Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD).toString))
      .withColumnRenamed("_1", "Tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    binDF.cache()
    val binCountDF = sqlContext.sql("select Tag, Bin, count(*) as Count from binDF group by Tag, Bin")
    binCountDF.createOrReplaceTempView("binCountDF")
    binCountDF.cache()
    val finalDF = sqlContext.sql("select Bin, collect_list(distinct concat(Tag,' - ',Count)) as ListTagCount " +
      "from binCountDF group by Bin")
    finalDF.show()
    
  }
}

object Job2Main{
  def apply: Job2Main = new Job2Main()
}
