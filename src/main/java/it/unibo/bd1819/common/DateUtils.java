package it.unibo.bd1819.common;

import de.jollyday.HolidayManager;
import de.jollyday.util.CalendarUtil;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;

public final class DateUtils {
    private static final String UTC_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final HolidayManager HOLIDAY_MANAGER = HolidayManager.getInstance();
    private static final CalendarUtil CALENDAR_UTIL = new CalendarUtil();

    @Contract(pure = true)
    private DateUtils() {}

    @Contract("null -> null")
    @Nullable
    public static DateTime parseDateFromString(final @Nullable String utc) {
        return utc == null ? null : DateTime.parse(utc, DateTimeFormat.forPattern(UTC_DATE_TIME_FORMAT).withZoneUTC());
    }

    @Contract(pure = true)
    @Nullable
    public static String parseNullableDate(final @NotNull String nullableUtc) {
        return "NA".equals(nullableUtc) ? null : nullableUtc;
    }

    @Contract("null -> false")
    public static boolean isHoliday(final @Nullable DateTime date) {
        if (date == null) {
            return false;
        } else {
            final LocalDate localDate = date.toLocalDate();
            return CALENDAR_UTIL.isWeekend(localDate) || HOLIDAY_MANAGER.isHoliday(localDate);
        }
    }

    @Contract("null -> false")
    public static boolean isWorkday(final @Nullable DateTime date) {
        return date != null && !isHoliday(date);
    }
}
