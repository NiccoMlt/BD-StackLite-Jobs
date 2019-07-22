package it.unibo.bd1819.daysproportion.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTriplet implements WritableComparable<TextTriplet> {
    private String tag;
    private long count;
    private double proportion;

    /** No argument constructor, required by Hadoop. */
    public TextTriplet() {
    }

    public TextTriplet(final String tag, final double proportion, final long count) {
        set(tag, proportion, count);
    }

    public void set(String tag, double proportion, long count) {
        this.tag = tag;
        this.count = count;
        this.proportion = proportion;
    }

    public String getTag() {
        return tag;
    }

    public long getCount() {
        return count;
    }

    public double getProportion() {
        return proportion;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeUTF(this.tag);
        out.writeDouble(this.proportion);
        out.writeLong(this.count);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.tag = in.readUTF();
        this.proportion = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public int hashCode() {
        /*
            This hashcode function is important as it is used by the custom
            partitioner for this class.
        */
        return (int) (tag.hashCode() * 163 + count + proportion);
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof TextTriplet) {
            TextTriplet tp = (TextTriplet) o;
            return tag.equals(tp.tag) && count == (tp.count) && proportion == (tp.proportion);
        }
        return false;
    }

    @Override
    public String toString() {
        return tag + "," + proportion + "," + count;
    }

    /*
     LHS in the conditional statement is the current key
     RHS in the conditional statement is the previous key
     When you return a negative value, it means that you are exchanging
     the positions of current and previous key-value pair
     Returning 0 or a positive value means that you are keeping the
     order as it is
    */
    @Override
    public int compareTo(final TextTriplet tp) {
        // Here my natural is tag and I don't even take it into
        // consideration.

        // So as you might have concluded, I am sorting count,F,proportion descendingly.
        if (this.count != tp.count) {
            if (this.count < tp.count) {
                return 1;
            } else {
                return -1;
            }
        }
        if (this.proportion != tp.proportion) {
            if (this.proportion < tp.proportion) {
                return 1;
            } else {
                return -1;
            }
        }
        return 0;
    }
}
