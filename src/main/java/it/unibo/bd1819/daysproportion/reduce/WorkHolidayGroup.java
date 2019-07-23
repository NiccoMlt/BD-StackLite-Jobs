package it.unibo.bd1819.daysproportion.reduce;


import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WorkHolidayGroup extends Reducer<BooleanWritable, LongWritable, BooleanWritable, Text> {
    private final transient Text list = new Text();

    @Override
    protected void reduce(final BooleanWritable key, final Iterable<LongWritable> values, final Context context) throws IOException, InterruptedException {
        final StringBuilder builder = new StringBuilder();

        for (final LongWritable val : values) {
            builder.append(val.toString()).append(",");
        }

        list.set(builder.toString());

        context.write(key, list);
    }
}
