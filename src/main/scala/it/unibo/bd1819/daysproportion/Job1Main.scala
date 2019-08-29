package it.unibo.bd1819.daysproportion

import it.unibo.bd1819.common.{Configuration, DateUtils, JobMainAbstract}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

class Job1Main extends JobMainAbstract {

  def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    this.configureEnvironment(sc, conf, sqlCont)
    import sqlCont.implicits._

    /* Select only Id and CreationDate columns from the questions DF, and then map the second one
     * into a boolean that will represent weather that date is a workday (true) or not (false).
     * Then create a DF with this information contained
     */
    val onlyDateDF = sqlContext.sql("select Id, CreationDate from questions")
      .map(row => (row.getString(0), DateUtils.isWorkday(DateUtils.parseDateFromString(row.getString(1)))))
      .withColumnRenamed("_1", "Id")
      .withColumnRenamed("_2", "IsWorkDay")
    
    /* Join the previously obtained DF with the question_tags DF, to obtain a resulting DF that
     * shows the relationship between the tags and the boolean value (IsWorkDay) that tells if 
     * the creation date of that answer is a workday or not.
     * Then drop the column of the Ids, will not be useful from now on
     */
    val dateAndTagDF = this.questionTagsDF.join(onlyDateDF, "Id").drop("Id")
    dateAndTagDF.createOrReplaceTempView("dateAndTagDF")
    dateAndTagDF.cache()
    
    /* Create a Sequence that represents the current columns names */
    val columnNamesToSelect = Seq("tag", "IsWorkDay")
    
    /* Add to the DF a column that represents how many questions with a specific Tag appear
     * in the Data Frame.
     */
    val countDF = dateAndTagDF
      .select(columnNamesToSelect.map(c => col(c)): _*)
      .groupBy("tag")
      .agg(count("IsWorkDay")
        .as("Count"))
    countDF.cache()
    
    /* Create a DF that has, associated to every tag, the proportion between how many
     * questions with that tag have been posted on workdays and how many on holidays.
     * Proportion float number will only have 2 decimals.
     */
    val workHolyDF = sqlContext.sql("select tag, (round(" +
      "(cast(sum(case when IsWorkDay = true then 1 else 0 end) as float)) / " +
      "(cast(sum(case when IsWorkDay = false then 1 else 0 end) as float)), " +
      "2)) as Proportion " +
      "from dateAndTagDF group by tag")
    
    /* The final DF will be a join between the previous one and the DF withe count information
     * (the amount of questions posted for every tag).
     */
    val finalJoinDF = workHolyDF.join(countDF, "tag")
    
    /* Show the first 20 rows for this DF */
    finalJoinDF.show()
  }
}

object Job1Main {
  def apply: Job1Main = new Job1Main()
}

