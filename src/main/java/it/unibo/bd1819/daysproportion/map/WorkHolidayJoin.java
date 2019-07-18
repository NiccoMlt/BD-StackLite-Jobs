package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorkHolidayJoin extends Mapper<Text, Text, LongWritable, Text> {

    public static final String WHJ_PREFIX = "2";

    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] keyStr = key.toString().split(",");

        context.write(new LongWritable(Long.parseLong(keyStr[0])), new Text(WHJ_PREFIX + keyStr[1] + "," + value.toString()));
    }
}
