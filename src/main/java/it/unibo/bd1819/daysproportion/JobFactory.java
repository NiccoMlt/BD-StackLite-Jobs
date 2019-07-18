package it.unibo.bd1819.daysproportion;

import java.io.IOException;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.daysproportion.map.QuestionTagMap;
import it.unibo.bd1819.daysproportion.map.WorkHolidayJoin;
import it.unibo.bd1819.daysproportion.map.WorkHolidayMapper;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayGroup;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static it.unibo.bd1819.common.JobUtils.QUESTION_TAGS_INPUT_PATH;

public class JobFactory {

    private static final Path WORKDAY_HOLIDAY_PATH = new Path(JobUtils.GENERIC_OUTPUT_PATH + "workdayHoliday");
    private static final Path WORKDAY_HOLIDAY_JOIN_PATH = new Path(JobUtils.GENERIC_OUTPUT_PATH + "workdayHolidayJoin");

    /**
     * Job #1: Create a job to map StackOverflow full questions to pair with (id, isWorkday) as key and 1 as value.
     *
     * @param conf the job configuration
     *
     * @return a Hadoop Job.
     *
     * @throws IOException if something goes wrong in the I/O process
     */
    public static Job getWorkdayHolidayJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        JobUtils.deleteOutputFolder(fs, JobUtils.OUTPUT_PATH);
        JobUtils.deleteOutputFolder(fs, WORKDAY_HOLIDAY_PATH);

        final Job job = Job.getInstance(conf, "Map full questions to pair (id, isWorkday)");

        job.setJarByClass(Main.class);

        JobUtils.configureJobForKeyValue(job,
            TextInputFormat.class, WorkHolidayMapper.class, BooleanWritable.class, LongWritable.class,
            WorkHolidayGroup.class, BooleanWritable.class, Text.class, TextOutputFormat.class);

        job.setNumReduceTasks(2);

        TextInputFormat.addInputPath(job, JobUtils.QUESTIONS_INPUT_PATH);
        TextOutputFormat.setOutputPath(job, WORKDAY_HOLIDAY_PATH);

        return job;
    }

    public static Job getWorkdayHolidayJoinJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        JobUtils.deleteOutputFolder(fs, WORKDAY_HOLIDAY_JOIN_PATH);

        final Job job = Job.getInstance(conf, "Join questions and tags");

        job.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job, QUESTION_TAGS_INPUT_PATH, TextInputFormat.class, QuestionTagMap.class);
        MultipleInputs.addInputPath(job, WORKDAY_HOLIDAY_PATH, KeyValueTextInputFormat.class, WorkHolidayJoin.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        JobUtils.configureReducer(job, WorkHolidayJoinReducer.class, Text.class, BooleanWritable.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, WORKDAY_HOLIDAY_JOIN_PATH);

        return job;
    }

}
