package it.unibo.bd1819.daysproportion;

import it.unibo.bd1819.common.JobUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

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
    public int run(final String[] args) throws Exception {
        final List<Job> jobs = new ArrayList<>();
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

        final JobFactory jobFactory = new JobFactory(inputPath, outputPath, conf);

        jobs.add(jobFactory.getWorkdayHolidayJoinJob());
        jobs.add(jobFactory.getDayProportionsJob());

        for (final Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

        return jobFactory.getSortJob().waitForCompletion(true) ? 0 : 1;
    }
}
