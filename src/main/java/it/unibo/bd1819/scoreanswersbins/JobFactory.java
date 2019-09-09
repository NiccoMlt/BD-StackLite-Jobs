package it.unibo.bd1819.scoreanswersbins;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.scoreanswersbins.map.BinMap;
import it.unibo.bd1819.scoreanswersbins.map.QuestionTagMap;
import it.unibo.bd1819.scoreanswersbins.map.ScoreCountTagMap;
import it.unibo.bd1819.scoreanswersbins.reduce.BinCountReducer;
import it.unibo.bd1819.scoreanswersbins.reduce.ScoreCountTagJoin;
import it.unibo.bd1819.scoreanswersbins.sort.BinPartitioner;
import it.unibo.bd1819.scoreanswersbins.sort.BinSortMapper;
import it.unibo.bd1819.scoreanswersbins.sort.BinSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static it.unibo.bd1819.common.JobUtils.*;

public class JobFactory {
    private static final String JOB_NAME = "scoreanswersbin";
    private static final String FIRST_TASK_NAME = "ScoreAnswerCountJoin";
    private static final String SECOND_TASK_NAME = "GetBinJob";

    private final String inputPath;
    private final String outputPath;
    private final Configuration conf;

    public JobFactory(final String inputPath, final String outputPath, final Configuration conf) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.conf = conf;
    }

    public Job getScoreAnswerCountJoinJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path jobOutputPath = getTaskOutputPath(outputPath, JOB_NAME, FIRST_TASK_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Join questions and tags storing Score and Answer Count");

        job.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job,
            getQuestionTagsInputPath(inputPath), TextInputFormat.class, QuestionTagMap.class);
        MultipleInputs.addInputPath(job,
            getQuestionsInputPath(inputPath), TextInputFormat.class, ScoreCountTagMap.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        JobUtils.configureReducer(job, ScoreCountTagJoin.class, Text.class, Text.class, TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, jobOutputPath);

        return job;
    }

    public Job getBinsJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path jobOutputPath = getTaskOutputPath(outputPath, JOB_NAME, SECOND_TASK_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Split tags in bins in relation to Score and Answer Count");

        job.setJarByClass(Main.class);

        KeyValueTextInputFormat.addInputPath(job, getTaskOutputPath(outputPath, JOB_NAME, FIRST_TASK_NAME));

        JobUtils.configureJobForKeyValue(job, KeyValueTextInputFormat.class, BinMap.class, Text.class, LongWritable.class,
            BinCountReducer.class, Text.class, LongWritable.class, TextOutputFormat.class);
        
        job.setCombinerClass(BinCountReducer.class);

        TextOutputFormat.setOutputPath(job, jobOutputPath);

        return job;
    }

    public Job getSortJob() throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        final Path jobOutputPath = getJobOutputPath(outputPath, JOB_NAME);
        deleteOutputFolder(fs, jobOutputPath);

        final Job job = Job.getInstance(conf, "Sort output");

        job.setJarByClass(Main.class);

        KeyValueTextInputFormat.addInputPath(job, getTaskOutputPath(outputPath, JOB_NAME, SECOND_TASK_NAME));

        JobUtils.configureJobForKeyValue(job, KeyValueTextInputFormat.class, BinSortMapper.class, Text.class,
            Text.class, BinSortReducer.class, Text.class, Text.class, TextOutputFormat.class);
        
        TextOutputFormat.setOutputPath(job, jobOutputPath);

        job.setPartitionerClass(BinPartitioner.class);
        job.setNumReduceTasks(Bin.values().length);

        return job;
    }
}
