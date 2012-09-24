import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationNMapper 
    extends Mapper<LongWritable, Text, LongWritable, DoubleTriplet> {

    @Override public void map (LongWritable key,
                               Text value,
                               Context context)
        throws IOException, InterruptedException {

        String [] tuple = value.toString ().split (",");
        long j      = (long) Double.parseDouble (tuple [0]);
        long k      = (long) Double.parseDouble (tuple [1]);
        double n_jk = Double.parseDouble (tuple [2]);

        context.write (new LongWritable (j),
                       new DoubleTriplet (1, k, n_jk));
    }
}
