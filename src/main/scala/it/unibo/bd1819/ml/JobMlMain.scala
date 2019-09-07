package it.unibo.bd1819.ml

import it.unibo.bd1819.common.{Configuration, DFBuilder, JobMainAbstract}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class JobMlMain extends JobMainAbstract {

  override def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    import sqlCont.implicits._

    val data = DFBuilder
      .getQuestionsDF(sc, sqlCont)
      .drop("Id", "CreationDate", "ClosedDate", "OwnerUserId")
      .filter($"DeletionDate".contains("NA"))
      .drop("DeletionDate")
      .filter((!$"AnswerCount".contains("NA")).and(!$"AnswerCount".contains("-")))

    val score: RDD[Double] = data.select("Score").map(row => row.getString(0).toDouble).rdd
    val count: RDD[Double] = data.select("AnswerCount").map(row => row.getString(0).toDouble).rdd

    val scCorrelationP: Double = Statistics.corr(score, count, "pearson")
    val scCorrelationS: Double = Statistics.corr(score, count, "spearman")

    println(s"Pearson Score/Count correlation: $scCorrelationP")
    println(s"Spearman Score/Count correlation: $scCorrelationS")
  }
}

object JobMlMain {
  def apply(): JobMlMain = new JobMlMain()
}
