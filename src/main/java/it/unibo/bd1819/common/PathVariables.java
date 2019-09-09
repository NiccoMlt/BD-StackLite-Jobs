package it.unibo.bd1819.common;

public final class PathVariables {

    private PathVariables(){}
    
    public static final String GENERIC_HDFS_PREFIX = "hdfs://";
    public static final String PERSONAL_HOME_PATH = GENERIC_HDFS_PREFIX + "/user/lsemprini/";
    public static final String ABSOLUTE_HDFS_PATH = PERSONAL_HOME_PATH + "bigdata/dataset/";
    public static final String MAIN_OUTPUT_FOLDER = "output";
    public static final String QUESTION_TAGS = "question_tags.csv";
    public static final String QUESTION_TAGS_PATH = ABSOLUTE_HDFS_PATH + QUESTION_TAGS;
    public static final String QUESTIONS = "questions.csv";
    public static final String QUESTIONS_PATH = ABSOLUTE_HDFS_PATH + QUESTIONS;
    public static final String HIVE_DATABASE = "lsemprini_nmaltoni_stacklite_db6";
    public static final String JOB_TABLE_NAME = HIVE_DATABASE + ".FinalTableJob";
}
