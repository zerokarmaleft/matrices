import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
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

        Job job = new Job (getConf (), "Multiply matrices P=MN");
        job.setJarByClass (getClass ());

        Path mInputPath = new Path (args [0]);
        Path nInputPath = new Path (args [1]);
        Path outputPath = new Path (args [2]);

        MultipleInputs.addInputPath (job,
                                     mInputPath,
                                     TextInputFormat.class,
                                     MatrixMultiplicationMMapper.class);
        MultipleInputs.addInputPath (job,
                                     nInputPath,
                                     TextInputFormat.class,
                                     MatrixMultiplicationNMapper.class);
        FileOutputFormat.setOutputPath (job, outputPath);

        job.setMapOutputKeyClass (LongWritable.class);
        job.setMapOutputValueClass (DoubleTriplet.class);
        job.setReducerClass (MatrixMultiplicationValueProductReducer.class);
        job.setOutputKeyClass (LongWritable.class);

        return job.waitForCompletion (true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run (new MatrixMultiplication (), args);
        System.exit (result);
    }
}
