package it.unibo.bd1819.daysproportion.sort.compositekey;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class ActualKeyPartitioner extends Partitioner<CompositeKey, Text> {
    private final Logger logger = Logger.getLogger(this.getClass());
//    private TotalOrderPartitioner<Text, Text> partitioner = new TotalOrderPartitioner<>();
    private HashPartitioner<Text, Text> partitioner = new HashPartitioner<>();
    private Text newKey = new Text();

    @Override
    public int getPartition(final CompositeKey key, final Text value, final int numReduceTasks) {
        try {
            newKey.set(key.getTag());
            return partitioner.getPartition(newKey, value, numReduceTasks);
        } catch (final Exception e) {
            logger.warn(e.getMessage(), e);
            return (int) (Math.random() * numReduceTasks);
        }
    }
}
