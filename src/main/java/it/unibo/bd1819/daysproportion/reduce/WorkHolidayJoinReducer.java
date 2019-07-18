package it.unibo.bd1819.daysproportion.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import static it.unibo.bd1819.daysproportion.map.QuestionTagMap.QT_PREFIX;
import static it.unibo.bd1819.daysproportion.map.WorkHolidayJoin.WHJ_PREFIX;

public class WorkHolidayJoinReducer extends Reducer<LongWritable, Text, Text, Text> {
    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final List<String> tags = new ArrayList<>();
        final List<String> data = new ArrayList<>();

        for (final Text value : values) {
            final String prefix = String.valueOf(value.toString().charAt(0));

            switch (prefix) {
                case QT_PREFIX:
                    tags.add(value.toString().substring(1));
                    break;
                case WHJ_PREFIX:
                    data.add(value.toString().substring(1));
                    break;
                default:
                    logger.warn("Key " + key.toString() + " - Unexpected value: " + value.toString());
            }
        }

        for (final String tag : tags) {
            for (final String pair : data) {
                context.write(new Text(tag), new Text(pair));
            }
        }
    }
}
