package it.unibo.bd1819.daysproportion;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.daysproportion.comparator.TagBoolComparator;
import it.unibo.bd1819.daysproportion.map.QuestionTagMap;
import it.unibo.bd1819.daysproportion.map.SwitchSortMapper;
import it.unibo.bd1819.daysproportion.map.WorkHolidayMap;
import it.unibo.bd1819.daysproportion.reduce.SwitchSortReducer;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayJoin;
import it.unibo.bd1819.daysproportion.reduce.WorkHolidayProportionReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

import static it.unibo.bd1819.common.JobUtils.*;

public class JobFactory {

    private static final Path WORKDAY_HOLIDAY_JOIN_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayJoin");
    private static final Path WORKDAY_HOLIDAY_PROPORTION_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayProportion");

    /**
     * Job #1: Create a job to map StackOverflow full questions to tuples (id, isWorkday) and join with tags by question ID.
     *
     * @param conf the job configuration
     * @return a Hadoop Job.
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
    
    public static Job getSortingJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
        deleteOutputFolder(fs, partitionPath);

        final Job job = Job.getInstance(conf, "Sorting Job");

        job.setJarByClass(Main.class);
        
        // TODO
        
        return job;
    }

    public static Job getSortJob(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(conf);

        final Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
        deleteOutputFolder(fs, partitionPath);

        final Job sortJob = Job.getInstance(conf, "Sort Job");

        sortJob.setJarByClass(Main.class);

        sortJob.setMapperClass(SwitchSortMapper.class);
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);

        sortJob.setMapOutputKeyClass(Text.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setSortComparatorClass(TagBoolComparator.class);

        sortJob.setReducerClass(SwitchSortReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(sortJob, WORKDAY_HOLIDAY_PROPORTION_PATH);
        FileOutputFormat.setOutputPath(sortJob, OUTPUT_PATH);

        sortJob.setNumReduceTasks(3);
        TotalOrderPartitioner.setPartitionFile(sortJob.getConfiguration(), partitionPath);

        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(1, 1000);
        InputSampler.writePartitionFile(sortJob, sampler);

        sortJob.setPartitionerClass(TotalOrderPartitioner.class);

        return sortJob;
    }
}
