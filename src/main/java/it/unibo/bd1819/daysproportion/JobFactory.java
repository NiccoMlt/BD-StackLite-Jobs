package it.unibo.bd1819.daysproportion;

import java.io.IOException;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.daysproportion.map.FedeSortMapper;
import it.unibo.bd1819.daysproportion.map.QuestionTagMap;
import it.unibo.bd1819.daysproportion.map.SortMapper;
import it.unibo.bd1819.daysproportion.map.WorkHolidayMap;
import it.unibo.bd1819.daysproportion.reduce.FedeSortReducer;
import it.unibo.bd1819.daysproportion.reduce.SortReducer;
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
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import static it.unibo.bd1819.common.JobUtils.*;

public class JobFactory {

    private static final Path WORKDAY_HOLIDAY_JOIN_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayJoin");
    private static final Path WORKDAY_HOLIDAY_PROPORTION_PATH = new Path(GENERIC_OUTPUT_PATH + "workdayHolidayProportion");
    private static final String PARTITION_PATH = GENERIC_OUTPUT_PATH + "partition";
    private static final Path PARTITION_FOLDER_PATH = new Path(PARTITION_PATH);
    private static final Path PARTITION_FILE_PATH = new Path(PARTITION_PATH, "part.lst");

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

/*    public static Job getSortJob(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(conf);

        Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
//        JobUtils.deleteOutputFolder(fs, PARTITION_FOLDER_PATH);
        deleteOutputFolder(fs, partitionPath);

        final Job job = Job.getInstance(conf, "Sort output by proportion");

        job.setJarByClass(Main.class);

        JobUtils.configureMapper(job, KeyValueTextInputFormat.class, SortMapper.class, Text.class, Text.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);

        job.setPartitionerClass(TotalOrderPartitioner.class);
//        TotalOrderPartitioner.setPartitionFile(conf, PARTITION_FILE_PATH);
//        job.setSortComparatorClass(TagBoolComparator.class);
        InputSampler.writePartitionFile(job, new InputSampler.RandomSampler<>(1, 1000));
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionPath);

        JobUtils.configureReducer(job, SortReducer.class, Text.class, Text.class, TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, OUTPUT_PATH);

        return job;
    }*/

    public static Job getFedeSortJob(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        /*final FileSystem fs = FileSystem.get(conf);

        Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        JobUtils.deleteOutputFolder(fs, OUTPUT_PATH);
//        JobUtils.deleteOutputFolder(fs, PARTITION_FOLDER_PATH);
        JobUtils.deleteOutputFolder(fs, partitionPath);

        final Job job = Job.getInstance(conf, "Sort output by proportion");
        

        return job;*/

        final FileSystem fs = FileSystem.get(conf);
        final String GENERIC_OUTPUT_PATH = "hdfs:///user/nmaltoni/mapreduce/";
        
        final Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
        deleteOutputFolder(fs, partitionPath);

        final Job sortJob = Job.getInstance(conf, "Sort Job");

        sortJob.setJarByClass(Main.class);
        
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        
        sortJob.setMapOutputKeyClass(Text.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setReducerClass(SortReducer.class);
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