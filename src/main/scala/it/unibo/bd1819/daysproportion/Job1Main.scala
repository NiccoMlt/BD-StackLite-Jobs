package it.unibo.bd1819.daysproportion

import it.unibo.bd1819.JobConfigurator
import it.unibo.bd1819.common.DateUtils
import it.unibo.bd1819.scoreanswersbins.Configuration
import it.unibo.bd1819.utils.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

class Job1Main {

  var sqlContext : SQLContext = _

  def executeJob(sc: SparkContext, conf: Configuration, sqlc: SQLContext): Unit = {
    // If users has not specified partitions and tasks for each partitions jobs use default
    if (conf.partitions == 0) {
      sqlContext = JobConfigurator.getDefault(sqlc).getSetSqlContext
    } else {
      sqlContext = JobConfigurator(sqlc, conf).getSetSqlContext
    }
    import sqlc.implicits._
    val questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
    val questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
    val onlyDateDF = sqlContext.sql("select Id, CreationDate from questions")
      .map(row => (row.getString(0), DateUtils.isWorkday(DateUtils.parseDateFromString(row.getString(1)))))
      .withColumnRenamed("_1", "Id")
      .withColumnRenamed("_2", "IsWorkDay")
    val joinDF = questionTagsDF.join(onlyDateDF, "Id").drop("Id")
    val columnNamesToSelect = Seq("tag", "IsWorkDay")
    val countDF = joinDF
      .select(columnNamesToSelect.map(c => col(c)): _*)
      .groupBy("tag")
      .agg(count("IsWorkDay")
        .as("Count"))
    joinDF.createOrReplaceTempView("joinDF")
    val workHolyDF = sqlContext.sql("select tag, (round(" +
      "(cast(sum(case when IsWorkDay = true then 1 else 0 end) as float)) / " +
      "(cast(sum(case when IsWorkDay = false then 1 else 0 end) as float)), " +
      "2)) as Proportion " +
      "from joinDF group by tag")
    val finalJoinDF = workHolyDF.join(countDF, "tag")
    finalJoinDF.createOrReplaceTempView("finalJoinDF")
    finalJoinDF.show()
    //val definitiveTableName = "fnaldini_director_actors_db.Actor_Director_Table_definitive"

    //sqlContext.sql("drop table if exists " + definitiveTableName)
  }
}

object Job1Main{
  def apply: Job1Main = new Job1Main()
}

