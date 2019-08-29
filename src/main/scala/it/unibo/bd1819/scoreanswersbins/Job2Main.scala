package it.unibo.bd1819.scoreanswersbins

import it.unibo.bd1819.JobMainAbstract
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job2Main extends JobMainAbstract {

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._
    val scoreAnswersDF = sqlContext.sql("select Id, Score, AnswerCount from questions")
    val joinDF = questionTagsDF.join(scoreAnswersDF, "Id").drop("Id")
    joinDF.createOrReplaceTempView("joinDF")
    val binDF = sqlContext.sql("select tag, Score, AnswerCount from joinDF")
      .map(row => (row.getString(0),
        Bin.getBinFor(Integer.parseInt(if (row.getString(1) == "null" || row.getString(1) == "NA") "0" else row.getString(1)),
          Bin.DEFAULT_SCORE_THRESHOLD,
          Integer.parseInt(if (row.getString(2) == "null" || row.getString(2) == "NA") "0" else row.getString(2)),
          Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD).toString))
      .withColumnRenamed("_1", "tag")
      .withColumnRenamed("_2", "Bin")
    binDF.createOrReplaceTempView("binDF")
    val binCountDF = sqlContext.sql("select tag, Bin, count(*) as Count from binDF group by tag, Bin")
    binCountDF.createOrReplaceTempView("binCountDF")
    val finalDF = sqlContext.sql("select Bin, GROUP_CONCAT(distinct concat(tag,',',Count) separator ';') from binCountDF group by Bin")
    finalDF.show()

  }
}

object Job2Main {
  def apply: Job2Main = new Job2Main()
}
