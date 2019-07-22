package it.unibo.bd1819.daysproportion.sort.text;

import it.unibo.bd1819.daysproportion.sort.CompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class ActualKeyTextGroupingComparator extends WritableComparator {
    private final Logger logger = Logger.getLogger(getClass());

    protected ActualKeyTextGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(final WritableComparable w1, final WritableComparable w2) {
        if (w1 instanceof Text && w2 instanceof Text) {
            logger.trace("Comparing 2 CompositeKeys for grouping");
            return CompositeKey.fromText((Text) w1).getTag().compareTo((CompositeKey.fromText((Text) w2).getTag()));
        } else {
            logger.warn("Comparing 2 non-CompositeKey objects for grouping: " + w1.getClass() + " & " + w2.getClass());
            return super.compare(w1, w2);
        }
    }
}
