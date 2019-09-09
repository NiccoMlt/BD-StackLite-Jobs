package it.unibo.bd1819.daysproportion;

import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;
import static it.unibo.bd1819.common.JobUtils.getJobOutputPath;
import static it.unibo.bd1819.common.JobUtils.getQuestionTagsInputPath;
import static it.unibo.bd1819.common.JobUtils.getQuestionsInputPath;
import static it.unibo.bd1819.common.JobUtils.getTaskOutputPath;

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
import java.io.IOException;
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

public class JobFactory {
    private static final String JOB_NAME = "daysproportion";
    private static final String FIRST_TASK_NAME = "workdayHolidayJoin";
    private static final String SECOND_TASK_NAME = "workdayHolidayProportion";

    private final String inputPath;
    private final String outputPath;
    private final Configuration conf;

    public JobFactory(final String inputPath, final String outputPath, final Configuration conf) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.conf = conf;
    }

    /**
     * Job #1: Create a job to map StackOverflow full questions to tuples (id, isWorkday)
     * and join with tags by question ID.
     *
     * @return a Hadoop Job.
     *
     * @throws IOException if something goes wrong in the I/O process
     */
    public Job getWorkdayHolidayJoinJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path jobOutputPath = getTaskOutputPath(outputPath, JOB_NAME, FIRST_TASK_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Join questions and tags");

        job.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job,
            getQuestionTagsInputPath(inputPath), TextInputFormat.class, QuestionTagMap.class);
        MultipleInputs.addInputPath(job,
            getQuestionsInputPath(inputPath), TextInputFormat.class, WorkHolidayMap.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        JobUtils.configureReducer(job, WorkHolidayJoin.class,
            Text.class, BooleanWritable.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, jobOutputPath);

        return job;
    }

    public Job getDayProportionsJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path jobOutputPath = getTaskOutputPath(outputPath, JOB_NAME, SECOND_TASK_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Proportion between workdays and holidays by tags");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, getTaskOutputPath(outputPath, JOB_NAME, FIRST_TASK_NAME));

        JobUtils.configureReducer(job,
            WorkHolidayProportionReducer.class, Text.class, Text.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, jobOutputPath);

        return job;
    }

    public Job getSortJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        final Path jobOutputPath = getJobOutputPath(outputPath, JOB_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Secondary sort Job");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, getTaskOutputPath(outputPath, JOB_NAME, SECOND_TASK_NAME));
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

        FileOutputFormat.setOutputPath(job, jobOutputPath);
        
        return job;
    }
}
