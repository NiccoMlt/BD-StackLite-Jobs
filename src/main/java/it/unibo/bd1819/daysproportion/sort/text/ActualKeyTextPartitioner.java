package it.unibo.bd1819.daysproportion.sort.text;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class ActualKeyTextPartitioner extends Partitioner<Text, Text> {
    private final Logger logger = Logger.getLogger(this.getClass());
    private TotalOrderPartitioner<Text, Text> partitioner = new TotalOrderPartitioner<>();
    private Text newKey = new Text();

    @Override
    public int getPartition(final Text key, final Text value, final int numReduceTasks) {
        try {
            newKey.set(CompositeKey.fromText(key).getTag());
            return partitioner.getPartition(newKey, value, numReduceTasks);
        } catch (final Exception e) {
            logger.warn(e.getMessage(), e);
            return (int) (Math.random() * numReduceTasks);
        }
    }
}
