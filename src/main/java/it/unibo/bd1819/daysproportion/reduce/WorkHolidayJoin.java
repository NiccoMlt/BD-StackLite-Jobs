package it.unibo.bd1819.daysproportion.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import static it.unibo.bd1819.daysproportion.map.QuestionTagMap.QT_PREFIX;
import static it.unibo.bd1819.daysproportion.map.WorkHolidayMap.WHM_PREFIX;

public class WorkHolidayJoin extends Reducer<LongWritable, Text, Text, BooleanWritable> {
    private static final Logger logger = Logger.getLogger(WorkHolidayJoin.class);
    private static final int TUPLE_SIZE = 1;

    private final transient Text tagKey = new Text();
    private final transient BooleanWritable workdayValue = new BooleanWritable();

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final List<String> tags = new ArrayList<>();
        final List<Boolean> workdays = new ArrayList<>();

        for (final Text value : values) {
            final String prefix = String.valueOf(value.toString().charAt(0));

            switch (prefix) {
                case QT_PREFIX:
                    tags.add(value.toString().substring(1));
                    break;
                case WHM_PREFIX:
                    workdays.add(Boolean.parseBoolean(value.toString().substring(1)));
                    break;
                default:
                    logger.warn("Key " + key.toString() + " - Unexpected value: " + value.toString());
                    break;
            }
        }

        if (workdays.size() != TUPLE_SIZE) {
            logger.warn("Unexpected WHJ boolean quantity: " + workdays.size());
        }

        for (final String tag : tags) {
            for (final boolean workday : workdays) {
                tagKey.set(tag);
                workdayValue.set(workday);
                context.write(tagKey, workdayValue);
            }
        }
    }
}
