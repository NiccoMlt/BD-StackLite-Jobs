package it.unibo.bd1819.daysproportion.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The OutputKeyComparator is used to sort the values iterator that the reducer gets.
 * It should be noted, that although the RawComparator is used to sort the values iterator,
 * the data that gets passed into the comparator is the mapper key output.
 * This is the reason why we must put all data in the key as well as the value.
 * <p>
 * The key comparator must also enforce the value grouping comparator’s rules.
 */
public class SortOutputKeyComparator extends WritableComparator {
    public SortOutputKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}
