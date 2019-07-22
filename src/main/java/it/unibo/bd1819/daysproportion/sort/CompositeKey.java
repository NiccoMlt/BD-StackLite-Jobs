package it.unibo.bd1819.daysproportion.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.jetbrains.annotations.NotNull;

public class CompositeKey implements WritableComparable {
    private static final Text.Comparator FALLBACK_COMPARATOR = new Text.Comparator();
    private String tag;
    private double proportion;
    private long count;
    
    public CompositeKey() {
        this("", 0, 0);
    }

    public CompositeKey(final String tag, final double proportion, final long count) {
        this.tag = tag;
        this.proportion = proportion;
        this.count = count;
    }
    
    public static CompositeKey fromKeyValue(final Text key, final Text value) {
        return CompositeKey.fromString(key.toString() + "," + value.toString());
    }

    public static CompositeKey fromString(final @NotNull String string) {
//        final String[] firstSplit = string.split("\t");
//        final String[] secondSplit = firstSplit[1].split(",");
//        return new CompositeKey(firstSplit[0], Double.parseDouble(secondSplit[0]), Long.parseLong(secondSplit[1]));
        final String[] split = string.split(",");
        return new CompositeKey(split[0], Double.parseDouble(split[1]), Long.parseLong(split[2]));
    }

    public static CompositeKey fromText(final Text key) {
        return CompositeKey.fromString(key.toString());
    }

    public int compareToCompositeKey(final @NotNull CompositeKey obj) {
        final int compareTag = getTag().compareTo(obj.getTag());

        if (compareTag == 0) {
            final int compareProportion = Double.compare(getProportion(), obj.getProportion());
            
            if (compareProportion == 0) {
                return Long.compare(getCount(), obj.getCount());
            } else {
                return compareProportion;
            }
        } else {
            return compareTag;
        }
    }

    public int compareToText(final @NotNull Text obj) {
        return this.compareToCompositeKey(CompositeKey.fromString(obj.toString()));
    }

    @Override
    public int compareTo(final @NotNull Object obj) {
        if (obj instanceof Text) {
            return compareToText((Text) obj);
        } else if (obj instanceof CompositeKey) {
            return compareToCompositeKey((CompositeKey) obj);
        } else {
            if (this == obj) {
                return 0;
            } else {
                return Objects.compare(this, obj, FALLBACK_COMPARATOR);
            }
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, getTag());
        WritableUtils.writeString(out, String.valueOf(getProportion()));
        WritableUtils.writeString(out, String.valueOf(getCount()));
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        tag = WritableUtils.readString(in);
        proportion = Double.parseDouble(WritableUtils.readString(in));
        count = Long.parseLong(WritableUtils.readString(in));
    }

    public String getTag() {
        return tag;
    }

    public double getProportion() {
        return proportion;
    }

    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
//        return getTag() + "\t" + getProportion() + "," + getCount();
        return getTag() + "," + getProportion() + "," + getCount();
    }

    public Text toText() {
        return new Text(this.toString());
    }
}
