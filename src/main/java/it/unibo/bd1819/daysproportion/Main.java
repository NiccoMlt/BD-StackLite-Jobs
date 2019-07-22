package it.unibo.bd1819.daysproportion;

import it.unibo.bd1819.daysproportion.sort.CustomSortJob;
import it.unibo.bd1819.daysproportion.sort.SortJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

public class Main extends Configured implements Tool {

    /**
     * Launch the job.
     *
     * @param args command line args
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

        jobs.add(JobFactory.getWorkdayHolidayJoinJob(conf));
        jobs.add(JobFactory.getDayProportionsJob(conf));

        for (final Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

//        return JobFactory.getSecondarySortTextJob(conf).waitForCompletion(true) ? 0 : 1;
        return CustomSortJob.getSortJob(conf).waitForCompletion(true) ? 0 : 1;
    }
}
