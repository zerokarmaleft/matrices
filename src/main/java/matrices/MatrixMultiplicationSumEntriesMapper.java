import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationSumEntriesMapper 
    extends Mapper<LongWritable, Text, LongPair, DoubleWritable> {

    @Override public void map (LongWritable key,
                               Text value,
                               Context context)
        throws IOException, InterruptedException {

        System.err.printf ("Value: %s\n", value.toString ());
        String[] j_ikv  = value.toString ().split ("\t");
        String [] tuple = j_ikv [1].split (",");
        System.err.printf ("(i,k,v): %s,%s,%s\n",
                           tuple [0],
                           tuple [1],
                           tuple [2]);
        long   i = Long.parseLong (tuple [0]);
        long   k = Long.parseLong (tuple [1]);
        double v = Double.parseDouble (tuple [2]);

        context.write (new LongPair (i, k),
                       new DoubleWritable (v));
    }
}
