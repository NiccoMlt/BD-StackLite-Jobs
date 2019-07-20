package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import it.unibo.bd1819.common.Question;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** This mapper loads questions.csv file and outputs a tuple (questionID, prefix + workday/holiday boolean). */
public class WorkHolidayMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    public static final String WHM_PREFIX = "2";
    private static final Integer LINE_NUM_TO_DROP = 0;
    private static final String TEXT_TO_DROP = "Id,CreationDate,ClosedDate,DeletionDate,Score,OwnerUserId,AnswerCount";

    private final Text workday = new Text();
    private final LongWritable id = new LongWritable();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        if (key.get() != LINE_NUM_TO_DROP || !value.toString().equals(TEXT_TO_DROP)) {
            final Question q = Question.parseText(key, value);
            workday.set(WHM_PREFIX + q.isCreatedInWorkday());
            id.set(q.getId());
            context.write(id, workday);
        }
    }
}
