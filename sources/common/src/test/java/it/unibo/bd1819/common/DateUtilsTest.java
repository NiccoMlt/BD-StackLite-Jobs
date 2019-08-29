package it.unibo.bd1819.common;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class DateUtilsTest {

    private static final String VALID_DATE_FROM_CSV = "2009-12-30T03:32:07Z";
    private static final String YEAR = VALID_DATE_FROM_CSV.split("-")[0];
    private static final String MONTH = VALID_DATE_FROM_CSV.split("-")[1];
    private static final String DAY = VALID_DATE_FROM_CSV.split("-")[2].split("T")[0];
    private static final String HOUR = VALID_DATE_FROM_CSV.split("T")[1].split(":")[0];
    private static final String MINUTES = VALID_DATE_FROM_CSV.split("T")[1].split(":")[1];
    private static final String SECONDS = VALID_DATE_FROM_CSV.split("T")[1].split(":")[2].substring(0, 2);

    private static final String NULL_DATE_FROM_CSV = "NA";

    @Test
    @SuppressWarnings("ConstantConditions")
    public void parseDateFromString() {
        final DateTime validDate = DateUtils.parseDateFromString(VALID_DATE_FROM_CSV);
        assert validDate != null;
        Assert.assertEquals(validDate.getYear(), Integer.parseInt(YEAR));
        Assert.assertEquals(validDate.getMonthOfYear(), Integer.parseInt(MONTH));
        Assert.assertEquals(validDate.getDayOfMonth(), Integer.parseInt(DAY));
        Assert.assertEquals(validDate.getHourOfDay(), Integer.parseInt(HOUR));
        Assert.assertEquals(validDate.getMinuteOfHour(), Integer.parseInt(MINUTES));
        Assert.assertEquals(validDate.getSecondOfMinute(), Integer.parseInt(SECONDS));

        try {
            final DateTime invalidDate = DateUtils.parseDateFromString(NULL_DATE_FROM_CSV);
            assert invalidDate != null;
            Assert.fail("Null date " + NULL_DATE_FROM_CSV + " was parsed to " + invalidDate.toString());
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException || e instanceof IllegalArgumentException);
        }

        final DateTime nullDate = DateUtils.parseDateFromString(null);
        Assert.assertNull(nullDate);
    }

    @Test
    public void parseNullableDate() {
        Assert.assertEquals(VALID_DATE_FROM_CSV, DateUtils.parseNullableDate(VALID_DATE_FROM_CSV));
        Assert.assertNull(DateUtils.parseNullableDate(NULL_DATE_FROM_CSV));
    }

    @Test
    public void isHoliday() {
        Assert.assertTrue(DateUtils.isHoliday(DateUtils.parseDateFromString("2009-12-25T03:32:07Z")));
        Assert.assertTrue(DateUtils.isHoliday(DateUtils.parseDateFromString("2019-07-21T03:32:07Z")));
    }

    @Test
    public void isWorkday() {
        Assert.assertTrue(DateUtils.isWorkday(DateUtils.parseDateFromString(VALID_DATE_FROM_CSV)));
    }
}
