package it.unibo.bd1819.daysproportion.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class TextTriplet implements WritableComparable<TextTriplet> {
    private String tag;
    private long count;
    private double proportion;

    /** No argument constructor, required by Hadoop. */
    @SuppressWarnings("unused")
    public TextTriplet() {
        // This constructor is intentionally empty. Nothing special is needed here.
    }

    /**
     * Actual constructor.
     *
     * @param tag        the tag
     * @param proportion the proportion between workdays and holidays
     * @param count      the number of question with given tag
     */
    public TextTriplet(final String tag, final double proportion, final long count) {
        this.tag = tag;
        this.count = count;
        this.proportion = proportion;
    }

    public String getTag() {
        return this.tag;
    }

    public long getCount() {
        return this.count;
    }

    public double getProportion() {
        return this.proportion;
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
        return Objects.hash(getTag(), getCount(), getProportion());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TextTriplet)) {
            return false;
        }
        final TextTriplet that = (TextTriplet) o;
        return getCount() == that.getCount() &&
            Double.compare(that.getProportion(), getProportion()) == 0 &&
            getTag().equals(that.getTag());
    }

    @Override
    public String toString() {
        return tag + "," + proportion + "," + count;
    }

    @Override
    public int compareTo(final TextTriplet tp) {
        final int compareProportion = -Double.compare(this.getProportion(), tp.getProportion());

        if (compareProportion == 0) {
            final int compareCount = -Long.compare(this.getCount(), tp.getCount());
            if (compareCount == 0) {
                return this.getTag().compareTo(tp.getTag());
            } else {
                return compareCount;
            }
        } else {
            return compareProportion;
        }
    }
}
