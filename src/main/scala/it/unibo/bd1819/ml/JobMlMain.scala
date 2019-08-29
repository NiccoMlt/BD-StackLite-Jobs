package it.unibo.bd1819.ml

import it.unibo.bd1819.JobMainAbstract
import it.unibo.bd1819.scoreanswersbins.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class JobMlMain extends JobMainAbstract {
  override def executeJob(sc: SparkContext, conf: Configuration, sqlCont: SQLContext): Unit = {
    // TODO
    ???
  }
}

object JobMlMain {
  def apply: JobMlMain = new JobMlMain()
}
