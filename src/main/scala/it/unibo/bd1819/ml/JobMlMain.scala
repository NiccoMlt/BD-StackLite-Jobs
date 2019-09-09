package it.unibo.bd1819.ml

import it.unibo.bd1819.common.{DFBuilder, JobMainAbstract, PathVariables}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

class JobMlMain extends JobMainAbstract {

  override def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    configureEnvironment(sc, sqlCont)
    import sqlCont.implicits._
    
    val data = this.questionsDF
      .select("DeletionDate","Score","AnswerCount")
      .filter($"DeletionDate".contains("NA"))
      .drop("DeletionDate")
      .filter((!$"AnswerCount".contains("NA")).and(!$"AnswerCount".contains("-")))
    
    val score: RDD[Double] = data.select("Score").map(row => row.getString(0).toDouble).rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val count: RDD[Double] = data.select("AnswerCount").map(row => row.getString(0).toDouble).rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val scCorrelationP: Double = Statistics.corr(score, count, JobMlMain.PEARSON)
    val scCorrelationS: Double = Statistics.corr(score, count, JobMlMain.SPEARMAN)
    
    score.unpersist()
    count.unpersist()
    
    sc.parallelize(Seq((JobMlMain.PEARSON, scCorrelationP), (JobMlMain.SPEARMAN, scCorrelationS)), numSlices = 1)
      .saveAsTextFile(PathVariables.PERSONAL_HOME_PATH + "spark/")
  }

  override protected def configureEnvironment(sc: SparkContext, sqlCont: SQLContext): Unit = {
    dropTables(sqlCont)
    this.questionsDF = DFBuilder.getDF(PathVariables.QUESTIONS_PATH, sqlCont)
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
