import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatrixMultiplication 
    extends Configured implements Tool {

    @Override public int run (String [] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [options] %s\n\n",
                              getClass ().getSimpleName (),
                              "<M input> <N input> <output>");
            GenericOptionsParser.printGenericCommandUsage (System.err);
            return -1;
        }

        Job job = new Job (getConf (), "Multiply matrix entries");
        job.setJarByClass (getClass ());

        Path mInputPath = new Path (args [0]);
        Path nInputPath = new Path (args [1]);
        Path outputPath = new Path (args [2]);
        Path tempPath   = new Path ("/tmp/products");

        MultipleInputs.addInputPath (job,
                                     mInputPath,
                                     TextInputFormat.class,
                                     MatrixMultiplicationMMapper.class);
        MultipleInputs.addInputPath (job,
                                     nInputPath,
                                     TextInputFormat.class,
                                     MatrixMultiplicationNMapper.class);
        FileOutputFormat.setOutputPath (job, tempPath);

        job.setMapOutputKeyClass (LongWritable.class);
        job.setMapOutputValueClass (DoubleTriplet.class);
        job.setReducerClass (MatrixMultiplicationValueProductReducer.class);
        job.setOutputKeyClass (LongWritable.class);

        // first stage failed
        if (!job.waitForCompletion (true)) return -1;

        Job job2 = new Job (getConf (), "Sum matrix entries");
        job2.setJarByClass (getClass ());

        TextInputFormat.addInputPath (job2, tempPath);
        FileOutputFormat.setOutputPath (job2, outputPath);

        job2.setMapperClass (MatrixMultiplicationSumEntriesMapper.class);
        job2.setMapOutputKeyClass (LongPair.class);
        job2.setMapOutputValueClass (DoubleWritable.class);
        job2.setReducerClass (MatrixMultiplicationSumEntriesReducer.class);
        job2.setOutputKeyClass (DoubleWritable.class);

        return job2.waitForCompletion (true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run (new MatrixMultiplication (), args);
        System.exit (result);
    }
}
