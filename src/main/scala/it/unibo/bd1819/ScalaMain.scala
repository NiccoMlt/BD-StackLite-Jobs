package it.unibo.bd1819

import utils.DFBuilder._
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import org.rogach.scallop.ScallopConf

object ScalaMain extends App {

  var executors = 2
  var taskForExceutor = 4

  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder.getOrCreate.sqlContext
  val conf = new Conf(args)

  if(conf.executors.supplied) {
    executors = conf.executors()
  }

  if(conf.tasks.supplied) {
    taskForExceutor = conf.tasks()
  }
  val questionsDF = getQuestionsDF(sc, sqlContext)
  //val definitiveTableName = "fnaldini_director_actors_db.Actor_Director_Table_definitive"

  //sqlContext.sql("drop table if exists " + definitiveTableName)

  sqlContext.setConf("spark.sql.shuffle.partitions", (executors*taskForExceutor).toString)
  sqlContext.setConf("spark.default.parallelism", (executors*taskForExceutor).toString)
  questionsDF.show()
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

