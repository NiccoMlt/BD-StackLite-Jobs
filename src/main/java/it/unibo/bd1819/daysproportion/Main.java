package it.unibo.bd1819.daysproportion;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class Main {

    /**
     * Launch the job.
     *
     * @param args command line args
     *
     * @throws Exception if something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        final List<Job> jobs = new ArrayList<>();
        final Configuration conf = new Configuration();

        jobs.add(JobFactory.getWorkdayHolidayJob(conf));
        jobs.add(JobFactory.getWorkdayHolidayJoinJob(conf));

        for (final Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

        // createSortJob(conf).waitForCompletion(true);
    }
}
