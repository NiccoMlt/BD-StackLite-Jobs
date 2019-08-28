package it.unibo.bd1819.scoreanswersbins.map;

import it.unibo.bd1819.scoreanswersbins.Bin;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BinMap extends Mapper<Text, Text, Text, Text> {

    private final Text binValue = new Text("");

    @Override
    protected void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final int scoreThreshold = conf
            .getInt(Bin.SCORE_THRESHOLD_CONF, Bin.DEFAULT_SCORE_THRESHOLD);
        final int countThreshold = conf
            .getInt(Bin.ANSWERS_COUNT_THRESHOLD_CONF, Bin.DEFAULT_ANSWERS_COUNT_THRESHOLD);

        final String[] split = value.toString().split(",");
        final int score = Integer.parseInt(split[0]);
        final int count = Integer.parseInt(split[1]);

        binValue.set(Bin.getBinFor(score, scoreThreshold, count, countThreshold).name());
        context.write(key, binValue);
    }
}
