package it.unibo.bd1819.scoreanswersbins;

import it.unibo.bd1819.common.AbstractMain;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class Main extends AbstractMain {

    /**
     * Launch the job.
     *
     * @param args command line args
     *
     * @throws Exception if something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public List<Job> getMainJobs(final String inputPath, final String outputPath, final Configuration conf) throws IOException {
        final List<Job> jobs = new ArrayList<>();

        final JobFactory jobFactory = new JobFactory(inputPath, outputPath, conf);

        jobs.add(jobFactory.getScoreAnswerCountJoinJob());
        // TODO

        return jobs;
    }

    @Override
    public Job getSortJob(final String inputPath, final String outputPath, final Configuration conf) throws IOException {
        // TODO
        return null;
    }
}
