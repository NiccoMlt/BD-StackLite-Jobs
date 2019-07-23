package it.unibo.bd1819.daysproportion.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

class SortMapper extends Mapper<Text, Text, TextTriplet, Text> {
    private final Logger logger = Logger.getLogger(getClass());
    
    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
       final String[] valueSplit = value.toString().split(",");
       if (valueSplit.length != 2) {
           logger.warn("Unexpected pair: key: " + key.toString() + " ; value: " + value.toString());
       } else {
           final double proportion = Double.parseDouble(valueSplit[0]);
           final long count = Long.parseLong(valueSplit[1]);
           context.write(new TextTriplet(key.toString(), proportion, count), value);
       }
    }
}
