package it.unibo.bd1819.daysproportion.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<TextTriplet, Text> {

    @Override
    public int getPartition(final TextTriplet key, final Text value, final int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

