import java.io.IOException;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MatrixMultiplicationSumEntriesMapperTest {
    
    @Test
    public void processesValidRecord ()
        throws IOException, InterruptedException {

        // (i, j, m_ij)
        Text value = new Text ("0\t0,1,-1.0");

        new MapDriver<LongWritable, Text, LongPair, DoubleWritable> ()
            .withMapper (new MatrixMultiplicationSumEntriesMapper ())
            .withInputValue (value)
            .withOutput (new LongPair (0, 1),
                         new DoubleWritable (-1.0))
            .runTest ();
    }
}
