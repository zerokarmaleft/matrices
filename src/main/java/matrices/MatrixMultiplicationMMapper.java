import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMMapper 
    extends Mapper<LongWritable, Text, LongWritable, DoubleTriplet> {

    @Override public void map (LongWritable key,
                               Text value,
                               Context context)
        throws IOException, InterruptedException {

        String [] tuple = value.toString ().split (",");
        long i      = (long) Double.parseDouble (tuple [0]);
        long j      = (long) Double.parseDouble (tuple [1]);
        double m_ij = Double.parseDouble (tuple [2]);

        context.write (new LongWritable (j),
                       new DoubleTriplet (0, i, m_ij));
    }
}
