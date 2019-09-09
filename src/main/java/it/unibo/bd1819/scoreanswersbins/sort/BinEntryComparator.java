package it.unibo.bd1819.scoreanswersbins.sort;

import java.util.Comparator;
import java.util.Map;

/** Simple comparator class that checks firstly the key and then the value. */
public class BinEntryComparator implements Comparator<Map.Entry<String, Long>> {
    @Override
    public int compare(final Map.Entry<String, Long> o1, final Map.Entry<String, Long> o2) {
        final int compareCount = -Long.compare(o1.getValue(), o2.getValue());
        return compareCount == 0 ? o1.getKey().compareTo(o2.getKey()) : compareCount;
    }
}
