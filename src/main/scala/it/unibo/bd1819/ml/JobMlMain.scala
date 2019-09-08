package it.unibo.bd1819.ml

import it.unibo.bd1819.common.DFBuilder.getQuestionsDF
import it.unibo.bd1819.common.{DFBuilder, JobMainAbstract, PathVariables}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}

class JobMlMain extends JobMainAbstract {

  override def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    configureEnvironment(sc, sqlCont)
    import sqlCont.implicits._

    val data = this.questionsDF
      .drop("Id", "CreationDate", "ClosedDate", "OwnerUserId")
      .filter($"DeletionDate".contains("NA"))
      .drop("DeletionDate")
      .filter((!$"AnswerCount".contains("NA")).and(!$"AnswerCount".contains("-")))

    val score: RDD[Double] = data.select("Score").map(row => row.getString(0).toDouble).rdd
    val count: RDD[Double] = data.select("AnswerCount").map(row => row.getString(0).toDouble).rdd

    val scCorrelationP: Double = Statistics.corr(score, count, JobMlMain.PEARSON)
    val scCorrelationS: Double = Statistics.corr(score, count, JobMlMain.SPEARMAN)

    sqlCont.createDataFrame(sc
      .parallelize(Seq((JobMlMain.PEARSON, scCorrelationP), (JobMlMain.SPEARMAN, scCorrelationS)), numSlices = 1))
      .toDF("Coefficient", "value")
      .write
      .mode(SaveMode.Overwrite)
      .csv(PathVariables.PERSONAL_HOME_PATH + "spark/")
  }

  override protected def configureEnvironment(sc: SparkContext, sqlCont: SQLContext): Unit = {
    dropTables(sqlCont)
    this.questionsDF = getQuestionsDF(sc, sqlCont)
  }

  override protected def dropTables(sqlCont: SQLContext): Unit = {
    // override just to do nothing (disable default table drop)
  }
}

object JobMlMain {
  private val PEARSON = "pearson"
  private val SPEARMAN = "spearman"

  def apply(): JobMlMain = new JobMlMain()
}
