package it.unibo.bd1819.daysproportion.map;

import java.io.IOException;

import it.unibo.bd1819.common.Question;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class WorkHolidayMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {
public class WorkHolidayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // TODO: use shared Text and IntWritable

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        if (key.get() != 0 || !value.toString().contains("Id,CreationDate,ClosedDate,DeletionDate,Score,OwnerUserId,AnswerCount")) {
            final Question q = Question.parseText(key, value);

//        context.write(key, new BooleanWritable(q.isCreatedInWorkday()));
            context.write(new Text(q.getId() + "," + q.isCreatedInWorkday()), new IntWritable(1));

        }
    }
}
