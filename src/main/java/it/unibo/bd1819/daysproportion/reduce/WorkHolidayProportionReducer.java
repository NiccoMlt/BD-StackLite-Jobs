package it.unibo.bd1819.daysproportion.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WorkHolidayProportionReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger logger = Logger.getLogger(WorkHolidayProportionReducer.class);

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        long holidays = 0;
        long workdays = 0;

        for (final Text isWorkDay : values) {
            if (Boolean.parseBoolean(isWorkDay.toString())) {
                workdays++;
            } else {
                holidays++;
            }
        }

        final double proportion = (double) workdays / (double) holidays;

        if (Double.isNaN(proportion)) {
            logger.warn("Unexpected proportion for tag: " + key.toString());
        } else {
            // Filter out extreme results // TODO
            if (!Double.isInfinite(proportion) && proportion != 0.0) {
                context.write(key, new Text(String.format("%.2f", proportion) + "," + (holidays + workdays)));
            }
        }
    }
}
