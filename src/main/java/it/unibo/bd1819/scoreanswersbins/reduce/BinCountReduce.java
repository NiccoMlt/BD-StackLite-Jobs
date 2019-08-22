package it.unibo.bd1819.scoreanswersbins.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.Nullable;

public class BinCountReduce extends Reducer<Text, Text, Text, LongWritable> {
    private final Text pairKey = new Text();
    private final LongWritable countValue = new LongWritable();

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final Map<String, Long> counts = new HashMap<>();

        for (final Text value : values) {
            final String binName = value.toString();
            final @Nullable Long current = counts.get(binName);
            counts.put(binName, current == null ? 1 : current + 1);
        }

        for (final Map.Entry<String, Long> pair : counts.entrySet()) {
            pairKey.set(key.toString() + "," + pair.getKey());
            countValue.set(pair.getValue());
            context.write(pairKey, countValue);
        }
    }
}
