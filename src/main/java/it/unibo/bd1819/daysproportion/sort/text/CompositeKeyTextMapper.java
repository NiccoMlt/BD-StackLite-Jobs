package it.unibo.bd1819.daysproportion.sort.text;

import java.io.IOException;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompositeKeyTextMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(final Text key, final Text value, Context context) throws IOException, InterruptedException {
        context.write(CompositeKey.fromKeyValue(key, value).toText(), value);
    }
}
