package it.unibo.bd1819.ml

import it.unibo.bd1819.common.{Configuration, DFBuilder, JobMainAbstract}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class JobMlMain extends JobMainAbstract {

  override def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    import sqlCont.implicits._

    val data = DFBuilder
      .getQuestionsDF(sc, sqlCont, isTags = false)
      .drop("Id", "CreationDate", "ClosedDate", "OwnerUserId")
      .filter($"DeletionDate".contains("NA"))
      .drop("DeletionDate")
      .filter((!$"AnswerCount".contains("NA")).and(!$"AnswerCount".contains("-")))
      .cache()
    
//    val score: RDD[Double] = data.select("Score").map(row => row.getString(0).toDouble).rdd
//    val count: RDD[Double] = data.select("AnswerCount").map(row => row.getString(0).toDouble).rdd
    
//    val df = data.map( row => Vectors.dense(row.getString(0).toDouble, row.getString(1).toDouble) )
    val df: RDD[Vector] = data.rdd.map { r: Row => Vectors.dense(r.getString(0).toDouble, r.getString(1).toDouble) }
    val correlationP = Statistics.corr(df, "pearson")
    println(s"Pearson Score/Count correlation: ${correlationP.toString}")
    val correlationS = Statistics.corr(df, "spearman")
    println(s"Spearman Score/Count correlation: ${correlationS.toString}")
//    val scCorrelationP: Double = Statistics.corr(score, count, "pearson")
//    val csCorrelationP: Double = Statistics.corr(count, score, "pearson")
//    val scCorrelationS: Double = Statistics.corr(score, count, "spearman")
//    val csCorrelationS: Double = Statistics.corr(count, score, "spearman")

//    println(s"Pearson Score/Count correlation: $scCorrelationP")
//    println(s"Pearson Count/Score correlation: $csCorrelationP")
    // println(s"Spearman Score/Count correlation: $scCorrelationS")
    // println(s"Spearman Count/Score correlation: $csCorrelationS")
  }
}

object JobMlMain {
  def apply(): JobMlMain = new JobMlMain()
}
