package it.unibo.bd1819.scoreanswersbins.reduce;

import static it.unibo.bd1819.scoreanswersbins.map.QuestionTagMap.QT_PREFIX;
import static it.unibo.bd1819.scoreanswersbins.map.ScoreCountTagMap.SCTM_PREFIX;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;

public class ScoreCountTagJoin extends Reducer<LongWritable, Text, Text, Text> {
    private static final Logger logger = getLogger(ScoreCountTagJoin.class);

    private final transient Text tagKey = new Text();
    private final transient Text dataValue = new Text();

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context)
        throws IOException, InterruptedException {
        final List<String> tags = new ArrayList<>();
        final List<String> datas = new ArrayList<>();

        for (final Text value : values) {
            final String prefix = String.valueOf(value.toString().charAt(0));

            switch (prefix) {
                case QT_PREFIX:
                    tags.add(value.toString().substring(1));
                    break;
                case SCTM_PREFIX:
                    datas.add(value.toString().substring(1));
                    break;
                default:
                    logger.warn("Key " + key.toString() + " - Unexpected value: " + value.toString());
                    break;
            }
        }

        for (final String tag : tags) {
            for (final String data : datas) {
                tagKey.set(tag);
                dataValue.set(data);
                context.write(tagKey, dataValue);
            }
        }
    }
}
