package it.unibo.bd1819.daysproportion.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SwitchSortMapper extends Mapper<Text, Text, Text, Text> {
    
    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}
