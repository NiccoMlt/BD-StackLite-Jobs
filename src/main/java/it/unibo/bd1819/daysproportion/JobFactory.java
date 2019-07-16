package it.unibo.bd1819.daysproportion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class JobFactory {
    private static final String GENERIC_INPUT_PATH = "hdfs:///user/nmaltoni/dataset/";
    private static final String GENERIC_OUTPUT_PATH = "hdfs:///user/nmaltoni/mapreduce/";
    private static final String QUESTIONS_INPUT_PATH = GENERIC_INPUT_PATH + "questions.csv";
    private static final String MAIN_OUTPUT_PATH = GENERIC_OUTPUT_PATH + "output";
    private static final Path outputPath = new Path(MAIN_OUTPUT_PATH);
    private static final Path workdayHolidayPath = new Path(GENERIC_OUTPUT_PATH + "workdayHoliday");

    /**
     * Create a job to map StackOverflow full questions to pair (id, isWorkday).
     *
     * @param conf the job configuration
     * @return a Hadoop Job.
     * @throws IOException if something goes wrong in the I/O process
     */
    public static Job workdayHolidayJobFactory(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        
        deleteOutputFolder(fs, outputPath);
        deleteOutputFolder(fs, workdayHolidayPath);
        
        final Job job = Job.getInstance(conf, "Map full questions to pair (id, isWorkday)");

        KeyValueTextInputFormat.addInputPath(job, new Path(QUESTIONS_INPUT_PATH));
        
        // TODO
        
        return job;
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }
}
