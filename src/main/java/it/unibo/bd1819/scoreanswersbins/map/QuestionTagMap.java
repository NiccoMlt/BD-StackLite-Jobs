package it.unibo.bd1819.scoreanswersbins.map;

import it.unibo.bd1819.common.QuestionTag;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Drop first line of question_tags.csv and output (Id, 1Tag) tuples. */
public class QuestionTagMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    public static final String QT_PREFIX = "1";

    private static final Integer LINE_NUM_TO_DROP = 0;
    private static final String TEXT_TO_DROP = "Id,Tag";

    private final transient LongWritable id = new LongWritable();
    private final transient Text tag = new Text();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context)
        throws IOException, InterruptedException {
        if (!(key.get() <= LINE_NUM_TO_DROP && value.toString().equals(TEXT_TO_DROP))) {
            final QuestionTag qt = QuestionTag.parseText(value);
            id.set(qt.getId());
            tag.set(QT_PREFIX + qt.getTag());
            context.write(id, tag);
        }
    }
}
