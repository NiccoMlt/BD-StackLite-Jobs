package it.unibo.bd1819.scoreanswersbins.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BinCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    @Override
    protected void reduce(final Text key, final Iterable<LongWritable> values, final Context context) throws IOException, InterruptedException {
        long count = 0L;

        for (final LongWritable value : values) {
            count += value.get();
        }

        context.write(key, new LongWritable(count));
    }
}
