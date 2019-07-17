package it.unibo.bd1819.daysproportion;

import java.io.IOException;
import javax.annotation.Nullable;

import it.unibo.bd1819.daysproportion.map.WorkHolidayMapper;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
     *
     * @return a Hadoop Job.
     *
     * @throws IOException if something goes wrong in the I/O process
     */
    public static Job workdayHolidayJobFactory(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        // deleteOutputFolder(fs, outputPath);
        deleteOutputFolder(fs, workdayHolidayPath);

        final Job job = Job.getInstance(conf, "Map full questions to pair (id, isWorkday)");

        job.setJarByClass(Main.class);
        
        job.setMapperClass(WorkHolidayMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(WorkHolidayCounter.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
//        KeyValueTextInputFormat.addInputPath(job, new Path(QUESTIONS_INPUT_PATH));
//        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        TextInputFormat.addInputPath(job, new Path(QUESTIONS_INPUT_PATH));
        TextOutputFormat.setOutputPath(job, workdayHolidayPath);
        
        // TODO

        return job;
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }
}
