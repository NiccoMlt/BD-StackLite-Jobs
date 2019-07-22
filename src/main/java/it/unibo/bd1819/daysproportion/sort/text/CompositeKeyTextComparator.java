package it.unibo.bd1819.daysproportion.sort.text;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class CompositeKeyTextComparator extends WritableComparator {
    private final Logger logger = Logger.getLogger(getClass());

    protected CompositeKeyTextComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(final WritableComparable w1, final WritableComparable w2) {
        if (w1 instanceof Text && w2 instanceof Text) {
            logger.trace("Comparing 2 CompositeKeys for sorting");
            final CompositeKey ck1 = CompositeKey.fromText((Text) w1);
            final CompositeKey ck2 = CompositeKey.fromText((Text) w2);

            final int compareProportion = Double.compare(ck1.getProportion(), ck2.getProportion());
            if (compareProportion == 0) {
                final int compareCount = Long.compare(ck1.getCount(), ck2.getCount());
                if (compareCount == 0) {
                    return ck1.getTag().compareTo(ck2.getTag());
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
