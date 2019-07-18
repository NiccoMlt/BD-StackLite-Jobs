package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Drop first line of question_tags.csv and output (Id, 1Tag) tuples. The */
public class QuestionTagMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Integer LINE_NUM_TO_DROP = 0;
    private static final String TEXT_TO_DROP = "Id,Tag";

    public static final String QT_PREFIX = "1";

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        if (!(key.get() <= LINE_NUM_TO_DROP && value.toString().equals(TEXT_TO_DROP))) {
            final String[] data = value.toString().split(",");
            context.write(new LongWritable(Long.parseLong(data[0])), new Text(QT_PREFIX + data[1]));
        }
    }
}
