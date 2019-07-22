package it.unibo.bd1819.daysproportion.sort.compositekey;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class CompositeKeyComparator extends WritableComparator {
    private final Logger logger = Logger.getLogger(getClass());

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(final WritableComparable w1, final WritableComparable w2) {
        if (w1 instanceof CompositeKey && w2 instanceof CompositeKey) {
            logger.trace("Comparing 2 CompositeKeys for sorting");
            final int compareProportion = Double.compare(((CompositeKey) w1).getProportion(), ((CompositeKey) w2).getProportion());
            if (compareProportion == 0) {
                final int compareCount = Long.compare(((CompositeKey) w1).getCount(), ((CompositeKey) w2).getCount());
                if (compareCount == 0) {
                    return ((CompositeKey) w1).getTag().compareTo(((CompositeKey) w2).getTag());
                } else {
                    return compareCount;
                }
            } else {
                return compareProportion;
            }
        } else {
            logger.warn("Comparing 2 non-CompositeKey objects for sorting: " + w1.getClass() + " & " + w2.getClass());
            return super.compare(w1, w2);
        }
    }
}
