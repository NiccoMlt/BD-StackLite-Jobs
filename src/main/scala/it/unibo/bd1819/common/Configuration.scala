package it.unibo.bd1819.common

case class Configuration(var partitions: Int = 8, var parallelism: Int = 8, var memorySize: Int = 1) // max threshold (8192 MB)

object Configuration {
  def apply(args: List[String]): Configuration = {
    val c = new Configuration()
    c.partitions = args(1).toInt
    c.parallelism = args(2).toInt
    c.memorySize = args(3).toInt
    c
  }
}
