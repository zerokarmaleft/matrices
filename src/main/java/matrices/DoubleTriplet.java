import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleTriplet
    implements WritableComparable<DoubleTriplet> {

    private DoubleWritable first;
    private DoubleWritable second;
    private DoubleWritable third;

    public DoubleTriplet () {
        set (new DoubleWritable (), new DoubleWritable (), new DoubleWritable ());
    }

    public DoubleTriplet (double first, double second, double third) {
        set (new DoubleWritable (first),
             new DoubleWritable (second),
             new DoubleWritable (third));
    }

    public DoubleTriplet (DoubleWritable first, 
                          DoubleWritable second, 
                          DoubleWritable third) {
        set (first, second, third);
    }

    public void set (DoubleWritable first,
                     DoubleWritable second,
                     DoubleWritable third) {
        this.first  = first;
        this.second = second;
        this.third  = third;
    }

    public DoubleWritable getFirst () {
        return first;
    }

    public DoubleWritable  getSecond () {
        return second;
    }

    public DoubleWritable getThird () {
        return third;
    }

    @Override public void write (DataOutput out) throws IOException {
        first.write (out);
        second.write (out);
        third.write (out);
    }

    @Override public void readFields (DataInput in) throws IOException {
        first.readFields (in);
        second.readFields (in);
        third.readFields (in);
    }
    
    @Override public int hashCode () {
        return first.hashCode () + second.hashCode () + third.hashCode ();
    }

    @Override public boolean equals (Object o) {
        if (o instanceof DoubleTriplet) {
            DoubleTriplet t = (DoubleTriplet) o;
            return (first.equals (t.first) &&
                    second.equals (t.second) &&
                    third.equals (t.third));
        }

        return false;
    }

    @Override public int compareTo (DoubleTriplet t) {
        int result = first.compareTo (t.first);
        if (result != 0) return result;

        result = second.compareTo (t.second);
        if (result != 0) return result;

        return third.compareTo (t.third);
    }

    @Override public String toString () {
        return first + "," + second +  "," + third ;
    }
}
