package it.unibo.bd1819.daysproportion.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static it.unibo.bd1819.daysproportion.reduce.WorkHolidayProportionReducer.HOLIDAY_ONLY;
import static it.unibo.bd1819.daysproportion.reduce.WorkHolidayProportionReducer.WORKDAY_ONLY;

public class SwitchSortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (final Text value : values) {
            final String[] splitted = value.toString().split(",");
            final double proportion = Double.parseDouble(splitted[0]);
            final long count = Long.parseLong(splitted[1]);
            final String proportionString = Double.isInfinite(proportion) ?
                HOLIDAY_ONLY :
                proportion == 0 ?
                    WORKDAY_ONLY :
                    String.format("%.2f", proportion);

            context.write(value, new Text(proportionString + "," + count));
        }
    }
}
