package it.unibo.bd1819.common

class Configuration {

  var partitions: Int = 0
  var parallelism: Int = 0
  var memorySize: Int = 0

}

object Configuration {
  def apply(args: List[String]): Configuration = {
    val c = new Configuration()
    c.partitions = args(1).toInt
    c.parallelism = args(2).toInt
    c.memorySize = args(3).toInt
    c
  }

  def apply(): Configuration = new Configuration()

  def apply(partitions: Int, parallelism: Int, memory: Int): Configuration = {
    val c = new Configuration()
    c.partitions = partitions
    c.parallelism = parallelism
    c.memorySize = memory
    c
  }
}
