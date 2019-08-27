package it.unibo.bd1819.scoreanswerbins

import it.unibo.bd1819.common.DateUtils
import it.unibo.bd1819.utils.DFBuilder.getQuestionsDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

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
