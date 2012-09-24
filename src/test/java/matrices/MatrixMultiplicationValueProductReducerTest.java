import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class MatrixMultiplicationValueProductReducerTest {
    
    @Test
    public void returnsListOfValueProducts ()
        throws IOException, InterruptedException {

        new ReduceDriver<LongWritable, DoubleTriplet, LongWritable, Text>()
            .withReducer (new MatrixMultiplicationValueProductReducer ())
            .withInputKey (new LongWritable (0))
            .withInputValues (Arrays.asList (new DoubleTriplet (0, 1, 1),
                                             new DoubleTriplet (1, 0, 1),
                                             new DoubleTriplet (1, 1, 2)))
            .withOutput (new LongWritable (0),
                         new Text ("1.0,0.0,1.0"))
            .withOutput (new LongWritable (0),
                         new Text ("1.0,1.0,2.0"))
            .runTest ();
    }
}
