package it.unibo.bd1819.scoreanswersbins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    /**
     * Launch the job.
     *
     * @param args command line args
     *
     * @throws Exception if something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(final String[] args) throws Exception {
        final String inputPath = args[0];
        final String outputPath = args[1];
        
        // TODO
        return 0;
    }
}
