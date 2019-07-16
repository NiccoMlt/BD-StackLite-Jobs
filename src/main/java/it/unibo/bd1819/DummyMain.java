package it.unibo.bd1819;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DummyMain {
    /**
     * Launch the job.
     *
     * @param args command line args
     *
     * @throws Exception if something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "dummy job");

        final Path inputPath = new Path("hdfs:///user/nmaltoni/dataset/questions.csv");
        final Path outputPath = new Path("hdfs:///user/nmaltoni/mapreduce/dummy_out");
        final FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(DummyMain.class);
        job.setMapperClass(DummyMapper.class);

        job.setReducerClass(IntSumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class DummyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String str = value.toString();
            final StringTokenizer itr = new StringTokenizer(str, " " + '\t' + '\n' + '\r' + '\f' + ',', false);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (final IntWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(key, result);
        }
    }
}
