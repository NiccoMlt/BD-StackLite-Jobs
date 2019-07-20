package it.unibo.bd1819.daysproportion.comparator;

import it.unibo.bd1819.daysproportion.reduce.WorkHolidayProportionReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TagBoolComparator extends WritableComparator {
    private Logger logger = Logger.getLogger(this.getClass());
    
    public TagBoolComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (isNotSomethingNull(a) && isNotSomethingNull(b) && a instanceof Text && b instanceof Text) {
            logger.debug("Comparing Text objects");

            final String[] aSplit = a.toString().split(",");
            final String[] bSplit = b.toString().split(",");

            if (aSplit.length < 2 || bSplit.length < 2) {
                return super.compare(a, b);
            } else {
                return compareValues(aSplit[0], Long.parseLong(aSplit[1]), bSplit[0], Long.parseLong(bSplit[1]));
            }
        } else {
            logger.warn("Comparing WritableComparable objects");
            return super.compare(a, b);
        }
    }

    private int compareValues(final @NotNull String aProportion, final long aCount, final @NotNull String bProportion, final long bCount) {
        switch (aProportion) {
            case WorkHolidayProportionReducer.WORKDAY_ONLY:
                switch (bProportion) {
                    case WorkHolidayProportionReducer.WORKDAY_ONLY:
                        // Workday-only are both infinity, so compare counts
                        return Long.compare(aCount, bCount);
                    case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                        // Workday-only is infinity, so bigger than Holiday-only
                        return 1;
                    default:
                        // Workday-only and Holiday-only are considered smaller than anything else
                        return -1;
                }
            case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                switch (bProportion) {
                    case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                        // Holiday-only are both zero, so compare counts
                        return Long.compare(aCount, bCount);
                    case WorkHolidayProportionReducer.WORKDAY_ONLY:
                        // Workday-only is infinity, so bigger than Holiday-only
                        return 1;
                    default:
                        // Workday-only and Holiday-only are considered smaller than anything else
                        return -1;
                }
            default:
                int compareProportion = Double.compare(Double.parseDouble(aProportion), Double.parseDouble(bProportion));
                return compareProportion == 0 ? Long.compare(aCount, bCount) : compareProportion;
        }
    }

    private boolean isNotSomethingNull(final @Nullable WritableComparable a) {
        return a != null && !(a instanceof NullWritable);
    }
}
