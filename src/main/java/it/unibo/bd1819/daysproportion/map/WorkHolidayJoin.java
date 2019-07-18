package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorkHolidayJoin extends Mapper<Text, Text, LongWritable, Text> {

    public static final String WHJ_PREFIX = "2";

    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] ids = value.toString().split(",");

        for (final String id : ids) {
            context.write(new LongWritable(Long.parseLong(id)), new Text(WHJ_PREFIX + key.toString()));
        }
    }
}
