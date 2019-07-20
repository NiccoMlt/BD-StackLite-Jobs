package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
