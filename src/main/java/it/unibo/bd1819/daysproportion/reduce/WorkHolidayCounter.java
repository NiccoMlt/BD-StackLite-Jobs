package it.unibo.bd1819.daysproportion.reduce;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WorkHolidayCounter extends Reducer<Text, IntWritable, Text, IntWritable> {
    // TODO: use shared IntWritable
    
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        for (final IntWritable val : values) {
            sum += val.get();
        }
        
        context.write(key, new IntWritable(sum));
    }
}
