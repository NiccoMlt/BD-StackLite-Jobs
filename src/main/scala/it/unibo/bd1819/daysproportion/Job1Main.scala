package it.unibo.bd1819.daysproportion

import it.unibo.bd1819.JobMainAbstract
import it.unibo.bd1819.common.DateUtils
import it.unibo.bd1819.scoreanswersbins.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

class Job1Main extends JobMainAbstract {

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
   this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._
    val onlyDateDF = sqlContext.sql("select Id, CreationDate from questions")
      .map(row => (row.getString(0), DateUtils.isWorkday(DateUtils.parseDateFromString(row.getString(1)))))
      .withColumnRenamed("_1", "Id")
      .withColumnRenamed("_2", "IsWorkDay")
    val dateAndTagDF = this.questionTagsDF.join(onlyDateDF, "Id").drop("Id")
    dateAndTagDF.createOrReplaceTempView("dateAndTagDF")
    dateAndTagDF.cache()
    val columnNamesToSelect = Seq("tag", "IsWorkDay")
    val countDF = dateAndTagDF
      .select(columnNamesToSelect.map(c => col(c)): _*)
      .groupBy("tag")
      .agg(count("IsWorkDay")
        .as("Count"))
    countDF.cache()
    val workHolyDF = sqlContext.sql("select tag, (round(" +
      "(cast(sum(case when IsWorkDay = true then 1 else 0 end) as float)) / " +
      "(cast(sum(case when IsWorkDay = false then 1 else 0 end) as float)), " +
      "2)) as Proportion " +
      "from dateAndTagDF group by tag")
    val finalJoinDF = workHolyDF.join(countDF, "tag")
    finalJoinDF.show()
    //val definitiveTableName = "fnaldini_director_actors_db.Actor_Director_Table_definitive"

    //sqlContext.sql("drop table if exists " + definitiveTableName)
  }
}

object Job1Main{
  def apply: Job1Main = new Job1Main()
}

