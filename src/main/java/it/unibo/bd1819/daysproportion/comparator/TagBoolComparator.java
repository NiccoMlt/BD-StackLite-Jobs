package it.unibo.bd1819.daysproportion.comparator;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;
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

            if (aSplit.length < 2 || bSplit.length < 2 ||
                !NumberUtils.isCreatable(aSplit[0]) || !NumberUtils.isCreatable(bSplit[0])) {
                logger.warn("Unexpected Text objects to compare: a(" + aSplit.length + ") is " + a + " b(" + bSplit.length + ") is " + b);
                return super.compare(a, b);
            } else {
                return compareValues(Double.parseDouble(aSplit[0]), Long.parseLong(aSplit[1]),
                    Double.parseDouble(bSplit[0]), Long.parseLong(bSplit[1]));
            }
        } else {
            logger.warn("Comparing WritableComparable objects");
            return super.compare(a, b);
        }
    }

    private int compareValues(final double aProportion, final long aCount, final double bProportion, final long bCount) {
        /*switch (aProportion) {
            case Double.POSITIVE_INFINITY:
                *//*switch (bProportion) {
                    case WorkHolidayProportionReducer.WORKDAY_ONLY:
                        // Workday-only are both infinity, so compare counts
                        return Long.compare(aCount, bCount);
                    case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                        // Workday-only is infinity, so bigger than Holiday-only
                        return 1;
                    default:
                        // Workday-only and Holiday-only are considered smaller than anything else
                        return -1;
                }*//*
            case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                *//*switch (bProportion) {
                    case WorkHolidayProportionReducer.HOLIDAY_ONLY:
                        // Holiday-only are both zero, so compare counts
                        return Long.compare(aCount, bCount);
                    case WorkHolidayProportionReducer.WORKDAY_ONLY:
                        // Workday-only is infinity, so bigger than Holiday-only
                        return 1;
                    default:
                        // Workday-only and Holiday-only are considered smaller than anything else
                        return -1;
                }*//*
            default:
                int compareProportion = Double.compare(Double.parseDouble(aProportion), Double.parseDouble(bProportion));
                return compareProportion == 0 ? Long.compare(aCount, bCount) : compareProportion;
        }*/
        if (aProportion == bProportion) {
            return Long.compare(aCount, bCount);
        } else if (aProportion == Double.POSITIVE_INFINITY && bProportion != Double.POSITIVE_INFINITY && bProportion != 0.0) {
            return 1;
        } else if (bProportion == Double.POSITIVE_INFINITY && aProportion != Double.POSITIVE_INFINITY && aProportion != 0.0) {
            return 0;
        } else {
            return Double.compare(aProportion, bProportion);
        }
    }

    private boolean isNotSomethingNull(final @Nullable WritableComparable a) {
        return a != null && !(a instanceof NullWritable);
    }
}
