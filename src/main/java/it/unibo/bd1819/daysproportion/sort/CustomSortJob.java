package it.unibo.bd1819.daysproportion.sort;

import java.io.IOException;

import it.unibo.bd1819.daysproportion.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import static it.unibo.bd1819.common.JobUtils.OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;
import static it.unibo.bd1819.daysproportion.JobFactory.WORKDAY_HOLIDAY_PROPORTION_PATH;

public class CustomSortJob {
    private CustomSortJob() {}

    public static Job getSortJob(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(conf);

        deleteOutputFolder(fs, OUTPUT_PATH);

        final Job job = Job.getInstance(conf, "Secondary sort Job");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);
        job.setMapperClass(CustomSortMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapOutputKeyClass(TextTriplet.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TextTriplet.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        return job;
    }

    private static class CustomSortMapper extends Mapper<Text, Text, TextTriplet, Text> {
        private final Logger logger = Logger.getLogger(getClass());
        
        @Override
        protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
           final String[] valueSplit = value.toString().split(",");
           if (valueSplit.length != 2) {
               logger.warn("Unexpected pair: key: " + key.toString() + " ; value: " + value.toString());
           } else {
               final double proportion = Double.parseDouble(valueSplit[0]);
               final long count = Long.parseLong(valueSplit[1]);
               context.write(new TextTriplet(key.toString(), proportion, count), value);
           }
        }
    }
}
