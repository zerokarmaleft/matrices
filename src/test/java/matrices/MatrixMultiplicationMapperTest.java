import java.io.IOException;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MatrixMultiplicationMapperTest {
    
    @Test
    public void processesValidRowColumnEntryInM()
        throws IOException, InterruptedException {

        // (i, j, m_ij)
        Text value = new Text ("0,1,-1");

        new MapDriver<LongWritable, Text, LongWritable, DoubleTriplet> ()
            .withMapper (new MatrixMultiplicationMMapper ())
            .withInputValue (value)
            .withOutput (new LongWritable (1), new DoubleTriplet (0, 0, -1))
            .runTest ();
    }

    @Test
    public void processesValidRowColumnEntryInN () 
        throws IOException, InterruptedException {

        // (j, k, n_jk)
        Text value = new Text ("1,0,3");

        new MapDriver<LongWritable, Text, LongWritable, DoubleTriplet> ()
            .withMapper (new MatrixMultiplicationNMapper ())
            .withInputValue (value)
            .withOutput (new LongWritable (1), new DoubleTriplet (1, 0, 3))
            .runTest ();
    }
}
