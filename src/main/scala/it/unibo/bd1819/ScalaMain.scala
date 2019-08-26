package it.unibo.bd1819

import breeze.linalg.sum
import it.unibo.bd1819.common.DateUtils
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import utils.DFBuilder._
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{ColumnName, Row, SQLContext, SparkSession}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions._

object ScalaMain extends App {

  var executors = 2
  var taskForExecutor = 4

  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder.getOrCreate.sqlContext
  import sqlContext.implicits._
  val conf = new Conf(args)

  if(conf.executors.supplied) {
    executors = conf.executors()
  }

  if(conf.tasks.supplied) {
    taskForExecutor = conf.tasks()
  }
  val questionsDF = getQuestionsDF(sc, sqlContext, isTags = false)
  val questionTagsDF = getQuestionsDF(sc, sqlContext, isTags = true)
  val onlyDateDF = sqlContext.sql("select Id, CreationDate from questions")
    .map(row => (row.getString(0), DateUtils.isWorkday(DateUtils.parseDateFromString(row.getString(1)))))
      .withColumnRenamed("_1", "Id")
      .withColumnRenamed("_2", "IsWorkDay")
  val joinDF = questionTagsDF.join(onlyDateDF, "Id").drop("Id")
  val columnNamesToSelect = Seq("tag", "IsWorkDay")
  val finalColumnNamesToSelect = Seq("tag", "IsWorkDay", "Count")
  val finalDF = joinDF
    .select(columnNamesToSelect.map(c => col(c)): _*)
    .groupBy("tag", "IsWorkDay")
    .agg(count("IsWorkDay")
      .as("Count"))
  joinDF.createOrReplaceTempView("joinDF")
  val superFinalDF = sqlContext.sql("select tag, sum(case when IsWorkDay = true then 1 else 0 end) WorkDays, " +
    "sum(case when IsWorkDay = false then 1 else 0 end) HolyDays " +
    "from joinDF group by tag")
    /*finalDF.select(finalColumnNamesToSelect.map(c => col(c)): _*)
    .where("IsWorkDay === true")
      .groupBy("tag", "IsWorkDay", "Count")
      .agg(count("*"))
      .as("WorkDays")
      */
  /*
    .agg(
      (when(col("IsWorkDay") === "true", count("IsWorkDay")) / 
        when(col("IsWorkDay") === "false", count("IsWorkDay")))
    .as("Proportion"))
      */ 
  superFinalDF.show()
  //val definitiveTableName = "fnaldini_director_actors_db.Actor_Director_Table_definitive"

  //sqlContext.sql("drop table if exists " + definitiveTableName)

  sqlContext.setConf("spark.sql.shuffle.partitions", (executors*taskForExecutor).toString)
  sqlContext.setConf("spark.default.parallelism", (executors*taskForExecutor).toString)
}

/**
  * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
  *
  * @param arguments the programs arguments as an array of strings.
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val executors = opt[Int]()
  val tasks = opt[Int]()
  verify()
}

