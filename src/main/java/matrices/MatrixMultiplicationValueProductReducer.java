import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationValueProductReducer 
    extends Reducer<LongWritable, DoubleTriplet, LongWritable, Text> {
    
    @Override protected void reduce (LongWritable key,
                                     Iterable<DoubleTriplet> values,
                                     Context context)
        throws IOException, InterruptedException {

        ArrayList<DoubleTriplet> mTuples = new ArrayList<DoubleTriplet>();
        ArrayList<DoubleTriplet> nTuples = new ArrayList<DoubleTriplet>();
        for (DoubleTriplet value : values) {
            if (value.getFirst().get() == 0) {
                mTuples.add (new DoubleTriplet(value.getFirst().get(),
                                               value.getSecond().get(),
                                               value.getThird().get()));
                System.err.printf("%g,%g,%g added to M\n",
                                  value.getFirst().get(),
                                  value.getSecond().get(),
                                  value.getThird().get());
            }
            if (value.getFirst().get() == 1) {
                nTuples.add (new DoubleTriplet(value.getFirst().get(),
                                               value.getSecond().get(),
                                               value.getThird().get()));
                System.err.printf("%g,%g,%g added to N\n",
                                  value.getFirst().get(),
                                  value.getSecond().get(),
                                  value.getThird().get());
            }
        }

        for (DoubleTriplet mTuple : mTuples) {
            for (DoubleTriplet nTuple : nTuples) {
                System.err.printf("%s cross %s\n",
                                  mTuple.toString(),
                                  nTuple.toString());

                double i    = mTuple.getSecond().get ();
                double k    = nTuple.getSecond().get ();
                double m_ij = mTuple.getThird ().get ();
                double n_jk = nTuple.getThird ().get ();

                context.write(key,
                              new Text(i + "," +
                                       k + "," +
                                       (m_ij * n_jk)));
            }
        }
    }
}
