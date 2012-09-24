import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LongPair
    implements WritableComparable<LongPair> {

    private LongWritable first;
    private LongWritable second;

    public LongPair () {
        set (new LongWritable (), new LongWritable ());
    }

    public LongPair (long first, long second) {
        set (new LongWritable (first),
             new LongWritable (second));
    }

    public LongPair (LongWritable first, 
                     LongWritable second) {
        set (first, second);
    }

    public void set (LongWritable first,
                     LongWritable second) {
        this.first  = first;
        this.second = second;
    }

    public LongWritable getFirst () {
        return first;
    }

    public LongWritable  getSecond () {
        return second;
    }

    @Override public void write (DataOutput out) throws IOException {
        first.write (out);
        second.write (out);
    }

    @Override public void readFields (DataInput in) throws IOException {
        first.readFields (in);
        second.readFields (in);
    }
    
    @Override public int hashCode () {
        return first.hashCode () + second.hashCode ();
    }

    @Override public boolean equals (Object o) {
        if (o instanceof LongPair) {
            LongPair p = (LongPair) o;
            return (first.equals (p.first) &&
                    second.equals (p.second));
        }

        return false;
    }

    @Override public int compareTo (LongPair p) {
        int result = first.compareTo (p.first);
        if (result != 0) return result;

        return second.compareTo (p.second);
    }

    @Override public String toString () {
        return "(" + first + "," + second + ")";
    }
}
