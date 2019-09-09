package it.unibo.bd1819.common;

public final class PathVariables {
//    public static final String PERSONAL_HOME_PATH = "hdfs:///user/lsemprini/";
    public static final String PERSONAL_HOME_PATH = "hdfs:///user/nmaltoni/";
    public static final String GENERIC_OUTPUT_PATH = PERSONAL_HOME_PATH + "mapreduce/";
    public static final String GENERIC_INPUT_PATH = PERSONAL_HOME_PATH + "dataset/";
    static final String QUESTION_TAGS = "question_tags.csv";
    static final String QUESTIONS = "questions.csv";
    static final String MAIN_OUTPUT_FOLDER = "output";

    private PathVariables(){}

    public static final String GENERIC_HDFS_PREFIX = "hdfs://";
    public static final String ABSOLUTE_HDFS_PATH = "/user/lsemprini/bigdata/dataset/";
//    public static final String ABSOLUTE_HDFS_PATH = GENERIC_INPUT_PATH;
    public static final String QUESTION_TAGS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "question_tags.csv";
    public static final String QUESTIONS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "questions.csv";
    public static final String HIVE_DATABASE = "lsemprini_nmaltoni_stacklite_db6";
//    public static final String HIVE_DATABASE = "nmaltoni_stacklite_db";
    public static final String JOB_TABLE_NAME = HIVE_DATABASE + ".FinalTableJob";
}
