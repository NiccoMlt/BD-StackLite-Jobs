package it.unibo.bd1819.scoreanswersbins.sort;

import it.unibo.bd1819.scoreanswersbins.Bin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;

public class BinPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(final Text key, final Text value, final int numPartitions) {
        return Arrays.asList(Bin.values()).indexOf(Bin.valueOf(key.toString())) % numPartitions;
    }
}
