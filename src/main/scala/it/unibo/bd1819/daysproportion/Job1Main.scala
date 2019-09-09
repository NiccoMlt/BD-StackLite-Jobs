package it.unibo.bd1819.daysproportion

import it.unibo.bd1819.common.DFBuilder.getQuestionTagsDF
import it.unibo.bd1819.common.{DFBuilder, DateUtils, JobMainAbstract, PathVariables}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Job1Main extends JobMainAbstract {

  def executeJob(sc: SparkContext, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and CreationDate columns from the questions DF, and then map the second one
     * into a boolean that will represent weather that date is a workday (true) or not (false).
     * Then create a DF with this information contained
     */
    val onlyDateDF = this.questionsDF
      .map(row => (row.getString(0), DateUtils.isWorkday(DateUtils.parseDateFromString(row.getString(1)))))
      .withColumnRenamed("_1", "Id")
      .withColumnRenamed("_2", "IsWorkDay")
    onlyDateDF.cache()
    
    /* Join the previously obtained DF with the question_tags DF, to obtain a resulting DF that
     * shows the relationship between the tags and the boolean value (IsWorkDay) that tells if 
     * the creation date of that answer is a workday or not.
     * Then drop the column of the Ids, will not be useful from now on
     */
    val dateAndTagDF = this.questionTagsDF.join(onlyDateDF, "Id").drop("Id")
    dateAndTagDF.createOrReplaceTempView("dateAndTagDF")
    
    /* Create a DF that has, associated to every tag, the proportion between how many
     * questions with that tag have been posted on workdays and how many on holidays.
     * Proportion float number will only have 2 decimals.
     * This DF will also have a column that represents how many questions with a specific Tag appear
     * in the Data Frame.
     */
    val finalDF = sqlCont.sql("select tag, (round(" +
      "(cast(sum(case when IsWorkDay = true then 1 else 0 end) as float)) / " +
      "(cast(sum(case when IsWorkDay = false then 1 else 0 end) as float)), " +
      "2)) as Proportion, " +
      "count(*) as Count " +
      "from dateAndTagDF group by tag " +
      "order by Proportion desc, Count desc")

    /* Save DF as Table on our Hive DB */
    finalDF.write/*.mode(SaveMode.Overwrite)*/.saveAsTable(job1FinalTableName)
  }


  override protected def configureEnvironment(sc: SparkContext, sqlCont: SQLContext): Unit = {
    dropTables(sqlCont)

    this.questionsDF = DFBuilder.getDF(PathVariables.QUESTIONS_PATH, sqlCont).select("Id", "CreationDate")
    this.questionTagsDF = getQuestionTagsDF(sc, sqlCont)
  }

  override protected def dropTables(sqlCont: SQLContext): Unit = {
    sqlCont.sql("drop table if exists " + job1FinalTableName)
  }
}

object Job1Main {
  def apply(): Job1Main = new Job1Main()
}

