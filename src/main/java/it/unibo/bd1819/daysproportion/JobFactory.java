package it.unibo.bd1819.daysproportion;

import java.io.IOException;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.daysproportion.map.QuestionTagMap;
import it.unibo.bd1819.daysproportion.map.WorkHolidayMap;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayJoin;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayProportionReducer;
import it.unibo.bd1819.daysproportion.sort.FirstPartitioner;
import it.unibo.bd1819.daysproportion.sort.GroupComparator;
import it.unibo.bd1819.daysproportion.sort.KeyComparator;
import it.unibo.bd1819.daysproportion.sort.SortMapper;
import it.unibo.bd1819.daysproportion.sort.SortReducer;
import it.unibo.bd1819.daysproportion.sort.TextTriplet;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static it.unibo.bd1819.common.JobUtils.GENERIC_OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.QUESTIONS_INPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.QUESTION_TAGS_INPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;

public final class JobFactory {

    private static final Path WORKDAY_HOLIDAY_JOIN_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayJoin");
    private static final Path WORKDAY_HOLIDAY_PROPORTION_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayProportion");

    private JobFactory() {
    }

    /**
     * Job #1: Create a job to map StackOverflow full questions to tuples (id, isWorkday) and join with tags by question ID.
     *
     * @param conf the job configuration
     *
     * @return a Hadoop Job.
     *
     * @throws IOException if something goes wrong in the I/O process
     */
    public static Job getWorkdayHolidayJoinJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        deleteOutputFolder(fs, WORKDAY_HOLIDAY_JOIN_PATH);

        final Job job = Job.getInstance(conf, "Join questions and tags");

        job.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job, QUESTION_TAGS_INPUT_PATH, TextInputFormat.class, QuestionTagMap.class);
        MultipleInputs.addInputPath(job, QUESTIONS_INPUT_PATH, TextInputFormat.class, WorkHolidayMap.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        JobUtils.configureReducer(job, WorkHolidayJoin.class, Text.class, BooleanWritable.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, WORKDAY_HOLIDAY_JOIN_PATH);

        return job;
    }

    public static Job getDayProportionsJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        deleteOutputFolder(fs, WORKDAY_HOLIDAY_PROPORTION_PATH);

        final Job job = Job.getInstance(conf, "Get proportion between workdays and holidays by tags");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_JOIN_PATH);

        JobUtils.configureReducer(job, WorkHolidayProportionReducer.class, Text.class, Text.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);

        return job;
    }

    public static Job getSortJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        deleteOutputFolder(fs, OUTPUT_PATH);

        final Job job = Job.getInstance(conf, "Secondary sort Job");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);
        job.setMapperClass(SortMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapOutputKeyClass(TextTriplet.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // TODO: use something else

        job.setReducerClass(SortReducer.class);

        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        return job;
    }
}
