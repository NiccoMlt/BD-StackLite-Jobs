package it.unibo.bd1819.scoreanswersbins.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;

@Deprecated
public class SafeBinSortReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Map<String, Long>> map;

    @Override
    protected void setup(final Context context) {
        map = new HashMap<>();
    }

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) {
        @Nullable Map<String, Long> keyMap = map.get(key.toString());
        
        if (keyMap == null) keyMap = new TreeMap<>();
        
        // for each (tag,count) pair, store the tag in the map, aggregating counts if necessary
        for (final Text value : values) {
            final String[] split = value.toString().split(",");
            final @Nullable Long count = keyMap.get(split[0]);
            keyMap.put(split[0], count == null ? Long.parseLong(split[1]) : count + Long.parseLong(split[1]));
        }
        
        map.put(key.toString(), keyMap);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        final Comparator<Map.Entry<String, Long>> comparator = new BinEntryComparator();
        
        for (Map.Entry<String, Map<String, Long>> entry : map.entrySet()) {
            final List<Map.Entry<String, Long>> list = new ArrayList<>(entry.getValue().entrySet());
            Collections.sort(list, comparator);

            for (int i = 0; i < 10 && i < list.size(); i++) {
                context.write(
                    new Text(entry.getKey()), 
                    new Text(list.get(i).getKey() + "," + list.get(i).getValue().toString()));
            }
        }
    }
}
