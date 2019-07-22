package it.unibo.bd1819.daysproportion.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(TextTriplet.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        final TextTriplet ip1 = (TextTriplet) w1;
        final TextTriplet ip2 = (TextTriplet) w2;
        /*
         LHS in the conditional statement is the current key-value pair
         RHS in the conditional statement is the previous key-value pair
         When you return a negative value, it means that you are exchanging
         the positions of current and previous key-value pair
         If you are comparing strings, the string which ends up as the argument
         for the `compareTo` method turns out to be the previous key and the
         string which is invoking the `compareTo` method turns out to be the
         current key.
        */
        if (ip1.getCount() == ip2.getCount()) {
            return Double.compare(ip2.getProportion(), ip1.getProportion());
        } else {
            if (ip1.getCount() < ip2.getCount())
                return 1;
            else
                return -1;
        }
    }
}
