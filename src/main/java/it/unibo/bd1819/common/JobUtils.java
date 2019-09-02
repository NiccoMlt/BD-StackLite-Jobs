package it.unibo.bd1819.common;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public final class JobUtils {
    public static final String PERSONAL_HOME_PATH = "hdfs:///user/nmaltoni/";
    public static final String GENERIC_INPUT_PATH = PERSONAL_HOME_PATH + "dataset/";
    public static final String GENERIC_OUTPUT_PATH = PERSONAL_HOME_PATH + "mapreduce/";
    private static final String QUESTION_TAGS = "question_tags.csv";
    private static final String QUESTIONS = "questions.csv";
    private static final String MAIN_OUTPUT_FOLDER = "output";

    private JobUtils() {
    }

    public static <K1, V1, K2, V2> void configureMapper(final Job job,
                                                        final Class<? extends InputFormat<K1, V1>> inputFormat,
                                                        final Class<? extends Mapper<K1, V1, K2, V2>> mapper,
                                                        final Class<K2> mapOutputKeyClass,
                                                        final Class<V2> mapOutputValueClass) {
        job.setInputFormatClass(inputFormat);

        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
    }

    public static <K2, V2, K3, V3> void configureReducer(final Job job,
                                                         final Class<? extends Reducer<K2, V2, K3, V3>> reducer,
                                                         final Class<K3> outputKeyClass,
                                                         final Class<V3> outputValueClass,
                                                         final Class<? extends OutputFormat> outputFormat) {
        job.setOutputFormatClass(outputFormat);

        job.setReducerClass(reducer);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
    }

    public static <K1, V1, K2, V2, K3, V3>
    void configureJobForKeyValue(final Job job, final Class<? extends InputFormat<K1, V1>> inputFormat,
                                 final Class<? extends Mapper<K1, V1, K2, V2>> mapper,
                                 final Class<K2> mapOutputKeyClass, final Class<V2> mapOutputValueClass,
                                 final Class<? extends Reducer<K2, V2, K3, V3>> reducer,
                                 final Class<K3> outputKeyClass, final Class<V3> outputValueClass,
                                 final Class<? extends OutputFormat> outputFormat) {
        configureMapper(job, inputFormat, mapper, mapOutputKeyClass, mapOutputValueClass);
        configureReducer(job, reducer, outputKeyClass, outputValueClass, outputFormat);
    }

    public static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }

    public static Path getQuestionsInputPath(final String inputFolder) {
        return new Path(inputFolder + QUESTIONS);
    }

    public static Path getQuestionTagsInputPath(final String inputFolder) {
        return new Path(inputFolder, QUESTION_TAGS);
    }

    public static Path getTaskOutputPath(final String baseOutput, final String jobName, final String taskName) {
        return new Path(baseOutput + Path.SEPARATOR + jobName + Path.SEPARATOR + taskName);
    }

    public static Path getJobOutputPath(final String baseOutput, final String jobName) {
        return getTaskOutputPath(baseOutput, jobName, MAIN_OUTPUT_FOLDER);
    }
}
