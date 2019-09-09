package it.unibo.bd1819.common;

import de.jollyday.HolidayCalendar;
import de.jollyday.HolidayManager;
import de.jollyday.ManagerParameter;
import de.jollyday.ManagerParameters;
import de.jollyday.util.CalendarUtil;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;

public final class DateUtils {
    private static final String UTC_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final ManagerParameter PARAMETERS = ManagerParameters.create(HolidayCalendar.ITALY, null);
    private static final HolidayManager HOLIDAY_MANAGER = HolidayManager.getInstance(PARAMETERS);
    private static final CalendarUtil CALENDAR_UTIL = new CalendarUtil();

    @Contract(pure = true)
    private DateUtils() {
    }

    /**
     * Parse UTC date from string.
     *
     * @param utc the nullable date string in UTC format
     * @return the date, if any, or null otherwise
     */
    @Contract("null -> null")
    @Nullable
    public static DateTime parseDateFromString(final @Nullable String utc) {
        return utc == null
            ? null
            : DateTime.parse(utc, DateTimeFormat.forPattern(UTC_DATE_TIME_FORMAT).withZoneUTC());
    }

    /**
     * Parse an UTC date that could also be "NA" if null.
     *
     * @param nullableUtc the date string
     * @return the date itself, or null if it's NA
     */
    @Contract(pure = true)
    @Nullable
    public static String parseNullableDate(final @NotNull String nullableUtc) {
        return "NA".equals(nullableUtc) ? null : nullableUtc;
    }

    /**
     * Check if the date is an holiday.
     *
     * @param date the date to check
     * @return true if it is holiday, false otherwise
     */
    @Contract("null -> false")
    public static boolean isHoliday(final @Nullable DateTime date) {
        if (date == null) {
            return false;
        } else {
            final LocalDate localDate = date.toLocalDate();
            return CALENDAR_UTIL.isWeekend(localDate) || HOLIDAY_MANAGER.isHoliday(localDate);
        }
    }


    /**
     * Check if the date is a work day.
     *
     * @param date the date to check
     * @return true if it's a work day, or false if it's null or an holiday
     */
    @Contract("null -> false")
    public static boolean isWorkday(final @Nullable DateTime date) {
        return date != null && !isHoliday(date);
    }
}
