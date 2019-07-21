package it.unibo.bd1819.daysproportion.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The OutputValueGroupingComparator is used to determine which reducer the mapper output row should go to.
 * This RawComparator does not sort a reducer’s value iterator. 
 * Instead, it’s used to sort reducer input, so that the reducer knows when a new grouping starts.
 */
public class SortOutputValueGroupingComparator extends WritableComparator {
    public SortOutputValueGroupingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}
