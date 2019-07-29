package it.unibo.bd1819.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractMain extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final List<String> filteredArgs = new ArrayList<>();

        for (final String arg : args) {
            // If jar file is generated with this class as a main class, that param should be ignored
            if (!arg.equals(this.getClass().getName())) {
                filteredArgs.add(arg);
            }
        }

        final String inputPath = filteredArgs.size() > 0
            ? JobUtils.PERSONAL_HOME_PATH + filteredArgs.get(0)
            : JobUtils.GENERIC_INPUT_PATH;
        final String outputPath = filteredArgs.size() > 0
            ? JobUtils.PERSONAL_HOME_PATH + filteredArgs.get(1)
            : JobUtils.GENERIC_OUTPUT_PATH;

        final List<Job> jobs = getMainJobs(inputPath, outputPath, conf);

        for (final Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

        return getSortJob(inputPath, outputPath, conf).waitForCompletion(true) ? 0 : 1;
    }

    public abstract List<Job> getMainJobs(final String inputPath, final String outputPath, final Configuration conf)
        throws IOException;

    public abstract Job getSortJob(final String inputPath, final String outputPath, final Configuration conf)
        throws IOException;
}
