package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import it.unibo.bd1819.common.Question;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorkHolidayMapper extends Mapper<LongWritable, Text, BooleanWritable, LongWritable> {
    private static final Integer LINE_NUM_TO_DROP = 0;
    private static final String TEXT_TO_DROP = "Id,CreationDate,ClosedDate,DeletionDate,Score,OwnerUserId,AnswerCount";

    private BooleanWritable workday = new BooleanWritable();
    private LongWritable id = new LongWritable();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        if (key.get() != LINE_NUM_TO_DROP || !value.toString().contains(TEXT_TO_DROP)) {
            final Question q = Question.parseText(key, value);
            workday.set(q.isCreatedInWorkday());
            id.set(q.getId());
            context.write(workday, id);
        }
    }
}
