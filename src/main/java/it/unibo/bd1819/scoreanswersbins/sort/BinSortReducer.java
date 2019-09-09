package it.unibo.bd1819.scoreanswersbins.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;

public class BinSortReducer extends Reducer<Text, Text, Text, Text> {
    private final Text key = new Text();
    private final Map<String, Long> map = new HashMap<>();

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) {
        this.key.set(key);
        for (final Text value : values) {
            final String[] split = value.toString().split(",");
            final @Nullable Long count = map.get(split[0]);
            map.put(split[0], count == null ? Long.parseLong(split[1]) : count + Long.parseLong(split[1]));
        }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        final Comparator<Map.Entry<String, Long>> comparator = new BinEntryComparator();
        final List<Map.Entry<String, Long>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, comparator);
        for (int i = 0; i < 10 && i < list.size(); i++) {
            context.write(key, new Text(list.get(i).getKey() + "," + list.get(i).getValue().toString()));
        }
        list.clear();
        map.clear();
        super.cleanup(context);
    }
}
