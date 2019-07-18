package it.unibo.bd1819.common;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class JobUtils {

    public static final String GENERIC_INPUT_PATH = "hdfs:///user/nmaltoni/dataset/";
    public static final String GENERIC_OUTPUT_PATH = "hdfs:///user/nmaltoni/mapreduce/";
    private static final String MAIN_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "output";

    public static final Path QUESTION_TAGS_INPUT_PATH = new Path(GENERIC_INPUT_PATH + "question_tags.csv");
    public static final Path MINI_QUESTION_TAGS_INPUT_PATH = new Path(GENERIC_INPUT_PATH + "mini_question_tags.csv");
    public static final Path QUESTIONS_INPUT_PATH = new Path(GENERIC_INPUT_PATH + "questions.csv");
    public static final Path MINI_QUESTIONS_INPUT_PATH = new Path(GENERIC_INPUT_PATH + "mini_questions.csv");
    public static final Path OUTPUT_PATH = new Path(MAIN_OUTPUT_PATH);

    private static <K1, V1, K2, V2> void configureMapper(final Job job, final Class<? extends InputFormat<K1, V1>> inputFormat,
                                                         final Class<? extends Mapper<K1, V1, K2, V2>> mapper,
                                                         final Class<K2> mapOutputKeyClass, final Class<V2> mapOutputValueClass) {
        job.setInputFormatClass(inputFormat);

        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
    }

    public static <K2, V2, K3, V3> void configureReducer(final Job job, final Class<? extends Reducer<K2, V2, K3, V3>> reducer,
                                                         final Class<K3> outputKeyClass, final Class<V3> outputValueClass,
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
}
