package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Drop first line of question_tags.csv and output (Id, 1Tag) tuples. The */
public class QuestionTagMap extends Mapper<LongWritable, Text, LongWritable, Text> {

    public static final String QT_PREFIX = "1";

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
//        if (!(key.toString().equals("Id") && value.toString().equals("Tag"))) {
//            context.write(new LongWritable(Long.parseLong(key.toString())), new Text(QT_PREFIX + value.toString()));
//        }
        if (!(key.get() <= 0 && value.toString().equals("Id,Tag"))) {
            final String[] data = value.toString().split(",");
            context.write(new LongWritable(Long.parseLong(data[0])), new Text(QT_PREFIX + data[1]));
        }
    }
}
