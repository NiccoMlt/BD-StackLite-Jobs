package it.unibo.bd1819.daysproportion.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(TextTriplet.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(final WritableComparable w1, final WritableComparable w2) {
        final TextTriplet ip1 = (TextTriplet) w1;
        final TextTriplet ip2 = (TextTriplet) w2;
        // Here we tell hadoop to group the keys by their natural key.
        return ip1.getTag().compareTo(ip2.getTag());
    }
}
