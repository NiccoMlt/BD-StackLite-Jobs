package it.unibo.bd1819.common

import org.apache.spark.sql.SQLContext

class JobConfigurator {

  var sqlContext : SQLContext = _

  def setSqlContext(sqlc: SQLContext): Unit = {
    sqlContext = sqlc
  }


  /**
   * Set the number of parallelisms to use
   * @param parallelism Number of parallelism
   *
   */
  def setParallelism(parallelism: Int): Unit = {
    sqlContext.setConf("spark.default.parallelism", (parallelism).toString)
  }

  /**
   * Set the number of partitions
   * @param partitions
   */
  def setPartitions(partitions: Int): Unit={
    sqlContext.setConf("spark.sql.shuffle.partitions", partitions.toString);
  }


  /**
   * Set the memory size available
   * @param memory
   */
  def setMemoryOffHeap(memory: Int): Unit = {
    sqlContext.setConf("spark.executor.memory", memory.toString + "g")
  }

  /**
   * Return a SQLContext set
   * @return
   */
  def getSetSqlContext: SQLContext = sqlContext

}


object JobConfigurator{

  def apply(context: SQLContext) : JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc
  }

  def apply(context: SQLContext, conf : Configuration): JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc.setPartitions(conf.partitions)
    jc.setParallelism(conf.parallelism)
    jc.setMemoryOffHeap(conf.memorySize)
    jc
  }

  def getDefault(context: SQLContext) : JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc.setParallelism(8)
    jc.setPartitions(8)
    jc.setMemoryOffHeap(11)
    jc
  }
}
