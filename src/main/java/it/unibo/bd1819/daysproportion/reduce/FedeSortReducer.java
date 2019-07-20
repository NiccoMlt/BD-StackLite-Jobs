package it.unibo.bd1819.daysproportion.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * For each record switch the DirectorID from the value to the Key and set the MoviesDirected key used for sorting as
 * a value.
 */
public class FedeSortReducer extends Reducer<IntWritable, Text, Text, Text> {
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for(Text value: values) {
            context.write(new Text(key.toString()), value);
        }
    }
/*
    *//**
     * Extract the Director Name and return it as a Text
     * @param oldValue the old value of the tuple
     * @return a Text containing the DirectorName
     *//*
    private Text extractNewKey(final Text oldValue) {
        return new Text("Director:" + oldValue.toString().split(CUSTOM_VALUE_SEPARATOR)[0]);
    }

    *//**
     * Create a new value for the tuple, containing the number of directed movies and the three actors with
     * the respective number of collaboration.
     * @param oldValue the old value of the tuples
     * @param moviesDirected the old key of the record, the number of movies directed
     * @return a Text containing the new value
     *//*
    private Text extractNewValue(final Text oldValue, final IntWritable moviesDirected) {
        final String directorID = oldValue.toString().split(CUSTOM_VALUE_SEPARATOR)[0] + CUSTOM_VALUE_SEPARATOR;
        return new Text("Movies Directed: " + (-moviesDirected.get()) + ", Most Frequently actors: " +
            oldValue.toString().replace(directorID,""));
    }*/
}
