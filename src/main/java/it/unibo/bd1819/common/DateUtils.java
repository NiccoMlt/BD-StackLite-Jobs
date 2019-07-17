package it.unibo.bd1819.common;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import de.jollyday.HolidayManager;
import de.jollyday.util.CalendarUtil;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;

public class DateUtils {
    private static final String UTC_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final HolidayManager HOLIDAY_MANAGER = HolidayManager.getInstance();
    private static final CalendarUtil CALENDAR_UTIL = new CalendarUtil();

    private DateUtils() {}

    @Nullable
    public static DateTime parseDateFromString(final @Nullable String utc) {
        return utc == null ? null : DateTime.parse(utc, DateTimeFormat.forPattern(UTC_DATE_TIME_FORMAT).withZoneUTC());
    }

    @Nullable
    public static String parseNullableDate(final @Nonnull String nullableUtc) {
        return nullableUtc.equals("NA") ? null : nullableUtc;
    }

    public static boolean isHoliday(final @Nullable DateTime date) {
        if (date == null) {
            return false;
        } else {
            final LocalDate localDate = date.toLocalDate();
            return CALENDAR_UTIL.isWeekend(localDate) || HOLIDAY_MANAGER.isHoliday(localDate);
        }
    }

    public static boolean isWorkday(final @Nullable DateTime date) {
        return !isHoliday(date);
    }
}
