package it.unibo.bd1819.daysproportion.sort;

import java.io.IOException;

import it.unibo.bd1819.daysproportion.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static it.unibo.bd1819.common.JobUtils.OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;
import static it.unibo.bd1819.daysproportion.JobFactory.WORKDAY_HOLIDAY_PROPORTION_PATH;

public class SortJob {
    private SortJob() {}

    public static Job getSortJob(final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

//        final Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
//        deleteOutputFolder(fs, partitionPath);

        final Job job = Job.getInstance(conf, "Secondary sort Job");

        job.setJarByClass(Main.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);
        job.setMapperClass(SortMapper.class);
        
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapOutputKeyClass(TextTriplet.class);
        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(TextTriplet.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1); // TODO: use something else
        
        job.setReducerClass(SortReducer.class);

        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        return job;
    }

}
