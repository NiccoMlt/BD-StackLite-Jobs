package it.unibo.bd1819.daysproportion.jobs;

import java.io.IOException;
import java.util.Iterator;

import it.unibo.bd1819.daysproportion.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

public class DummyJobFactory {
    private DummyJobFactory() {
        // Empty private constructor
    }

    public static Job buildDummyJob(final String[] args) throws IOException {
        final JobConf conf = new JobConf(Main.class);
        conf.setJobName("Dummy job");

        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        final FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setMapperClass(DummyJobMapper.class);
        conf.setReducerClass(DummyJobReducer.class);

        if (args.length > 2 && Integer.parseInt(args[2]) >= 0) {
            conf.setNumReduceTasks(Integer.parseInt(args[2]));
        } else {
            conf.setNumReduceTasks(1);
        }

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        return Job.getInstance(conf);
    }

    private static class DummyJobMapper implements Mapper {
        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            // TODO
        }

        @Override
        public void close() throws IOException {
            // TODO
        }

        @Override
        public void configure(JobConf job) {
            // TODO
        }
    }

    private static class DummyJobReducer implements Reducer {
        @Override
        public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            // TODO
        }

        @Override
        public void close() throws IOException {
            // TODO
        }

        @Override
        public void configure(JobConf job) {
            // TODO
        }
    }
}
