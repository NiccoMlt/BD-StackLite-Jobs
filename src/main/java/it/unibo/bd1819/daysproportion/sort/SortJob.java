package it.unibo.bd1819.daysproportion.sort;

import java.io.IOException;

import it.unibo.bd1819.daysproportion.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.log4j.Logger;

import static it.unibo.bd1819.common.JobUtils.GENERIC_OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.OUTPUT_PATH;
import static it.unibo.bd1819.common.JobUtils.deleteOutputFolder;
import static it.unibo.bd1819.daysproportion.JobFactory.WORKDAY_HOLIDAY_PROPORTION_PATH;

public class SortJob {
    private SortJob() {}

    public static Job getSortJob(final Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(conf);

        final Path partitionPath = new Path(GENERIC_OUTPUT_PATH + "partition", "part.lst");
        deleteOutputFolder(fs, OUTPUT_PATH);
        deleteOutputFolder(fs, partitionPath);

        final Job job = Job.getInstance(conf, "Secondary sort Job");

        job.setJarByClass(Main.class);

        job.setMapperClass(CompositeKeyMapper.class);
        KeyValueTextInputFormat.addInputPath(job, WORKDAY_HOLIDAY_PROPORTION_PATH);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        TextOutputFormat.setOutputPath(job, OUTPUT_PATH);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionPath);

        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(1, 1000);
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        return job;
    }

    private static String keyValueToCompositeKeyToString(final String key, final String value) {
        return key + "," + value;
    }

    private static String keyValueToCompositeKeyToString(final Text key, final Text value) {
        return keyValueToCompositeKeyToString(key.toString(), value.toString());
    }

    private static Text keyValueToCompositeKeyToText(final String key, final String value) {
        return new Text(keyValueToCompositeKeyToString(key, value));
    }

    private static Text keyValueToCompositeKeyToText(final Text key, final Text value) {
        return keyValueToCompositeKeyToText(key.toString(), value.toString());
    }

    private static String getTagFromCompositeKey(final String key) {
        return key.split(",")[0];
    }

    private static String getTagFromCompositeKey(final Text key) {
        return getTagFromCompositeKey(key.toString());
    }

    private static class CompositeKeyMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            context.write(keyValueToCompositeKeyToText(key, value), value);
        }
    }

    private static class ActualKeyPartitioner extends Partitioner<Text, Text> {
        private final Logger logger = Logger.getLogger(getClass());
                private TotalOrderPartitioner<Text, Text> partitioner = new TotalOrderPartitioner<>();
//        private HashPartitioner<Text, Text> partitioner = new HashPartitioner<>();
        private Text newKey = new Text();


        @Override
        public int getPartition(final Text key, final Text value, int numReduceTasks) {
            try {
                newKey.set(getTagFromCompositeKey(key));
                return partitioner.getPartition(newKey, value, numReduceTasks);
            } catch (final Exception e) {
                logger.warn(e.getMessage(), e);
                return (int) (Math.random() * numReduceTasks);
            }
        }
    }

    private static class CompositeKeyComparator extends WritableComparator {
        private final Logger logger = Logger.getLogger(getClass());

        protected CompositeKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(final WritableComparable w1, final WritableComparable w2) {
            if (w1 instanceof Text && w2 instanceof Text) {
                logger.trace("Comparing 2 Text CompositeKeys for sorting");
                final String[] w1Split = w1.toString().split(",");
                final String[] w2Split = w2.toString().split(",");
                final int compareProportion = -Double.compare(Double.parseDouble(w1Split[1]), Double.parseDouble(w2Split[1]));
                if (compareProportion == 0) {
                    final int compareCount = -Long.compare(Long.parseLong(w1Split[2]), Long.parseLong(w2Split[2]));
                    if (compareCount == 0) {
                        return w1Split[0].compareTo(w2Split[0]);
                    } else {
                        return compareCount;
                    }
                } else {
                    return compareProportion;
                }
            } else {
                logger.warn("Comparing 2 non-CompositeKey objects for sorting: " + w1.getClass() + " & " + w2.getClass());
                return super.compare(w1, w2);
            }
        }
    }

    private static class ActualKeyGroupingComparator extends WritableComparator {
        private final Logger logger = Logger.getLogger(getClass());

        protected ActualKeyGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(final WritableComparable w1, final WritableComparable w2) {
            if (w1 instanceof Text && w2 instanceof Text) {
                logger.trace("Comparing 2 Text CompositeKeys for grouping");
                return getTagFromCompositeKey((Text) w1).compareTo(getTagFromCompositeKey((Text) w2));
            } else {
                logger.warn("Comparing 2 non-Text CompositeKey objects for grouping: " + w1.getClass() + " & " + w2.getClass());
                return super.compare(w1, w2);
            }
        }
    }
}
