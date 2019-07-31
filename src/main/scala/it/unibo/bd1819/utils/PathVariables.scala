package it.unibo.bd1819.utils

object PathVariables {

  val GENERIC_HDFS_PREFIX = "hdfs://"
  val ABSOLUTE_HDFS_PATH = "/user/lsemprini/bigdata/project/dataset/dataset"
  val QUESTION_TAGS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH +"question_tags.csv"
  val QUESTIONS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "questions.csv"
}
