package it.unibo.bd1819.daysproportion.sort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<TextTriplet, Text, Text, Text> {

    @Override
    protected void reduce(TextTriplet key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        for (final Text value : values) {
            context.write(new Text(key.getTag()), value);
        }
    }
}
