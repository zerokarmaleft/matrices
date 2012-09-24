import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationSumEntriesReducer
    extends Reducer<LongPair, DoubleWritable, LongPair, DoubleWritable> {
    
    @Override protected void reduce (LongPair key,
                                     Iterable<DoubleWritable> values,
                                     Context context)
        throws IOException, InterruptedException {

        double sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get ();
        }

        context.write (key, new DoubleWritable (sum));
    }
}
