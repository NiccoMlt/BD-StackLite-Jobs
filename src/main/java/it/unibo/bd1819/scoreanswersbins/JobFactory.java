package it.unibo.bd1819.scoreanswersbins;

import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;
import static it.unibo.bd1819.common.JobUtils.getQuestionTagsInputPath;
import static it.unibo.bd1819.common.JobUtils.getQuestionsInputPath;
import static it.unibo.bd1819.common.JobUtils.getTaskOutputPath;

import it.unibo.bd1819.common.JobUtils;
import it.unibo.bd1819.scoreanswersbins.map.QuestionTagMap;
import it.unibo.bd1819.scoreanswersbins.map.ScoreCountTagMap;
import it.unibo.bd1819.scoreanswersbins.reduce.ScoreCountTagJoin;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobFactory {
    private static final String JOB_NAME = "scoreanswersbin";
    private static final String FIRST_TASK_NAME = "ScoreAnswerCountJoin";

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

    public Job getSortJob() throws IOException {
        // TODO
        return null;
    }
}
