package it.unibo.bd1819.common

object PathVariables {

  val GENERIC_HDFS_PREFIX = "hdfs://"
  val ABSOLUTE_HDFS_PATH = "/user/lsemprini/bigdata/project/dataset/dataset/"
  val QUESTION_TAGS_PATH: String = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH +"question_tags.csv"
  val QUESTIONS_PATH: String = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "questions.csv"
  val HIVE_DATABASE: String = "lsemprini_nmaltoni_stacklite_db"
  val JOB_TABLE_NAME: String = HIVE_DATABASE + ".FinalTableJob"
}
