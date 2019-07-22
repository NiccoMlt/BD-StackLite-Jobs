package it.unibo.bd1819.daysproportion.sort.compositekey;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class ActualKeyGroupingComparator extends WritableComparator {
    private final Logger logger = Logger.getLogger(getClass());

    protected ActualKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(final WritableComparable w1, final WritableComparable w2) {
        if (w1 instanceof CompositeKey && w2 instanceof CompositeKey) {
            logger.trace("Comparing 2 CompositeKeys for grouping");
            return ((CompositeKey) w1).getTag().compareTo(((CompositeKey) w2).getTag());
        } else {
            logger.warn("Comparing 2 non-CompositeKey objects for grouping: " + w1.getClass() + " & " + w2.getClass());
            return super.compare(w1, w2);
        }
    }
}
