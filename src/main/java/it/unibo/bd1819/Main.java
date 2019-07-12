package it.unibo.bd1819;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class Main {
    public static void main(String[] args) throws Exception {
        final List<Job> jobs = new ArrayList<>();
        final Configuration conf = new Configuration();

//        jobs.add(createDirectorsMovieJoin(conf));

        for (final Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

//        createSortJob(conf).waitForCompletion(true);
    }
}
