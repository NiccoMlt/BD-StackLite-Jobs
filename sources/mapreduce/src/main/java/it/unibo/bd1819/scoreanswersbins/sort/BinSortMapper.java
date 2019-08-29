package it.unibo.bd1819.scoreanswersbins.sort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BinSortMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] split = key.toString().split(",");
        // TODO improve
        context.write(new Text(split[1]), new Text(split[0] + "," + value.toString()));
    }
}
