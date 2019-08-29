package it.unibo.bd1819.common;

import static it.unibo.bd1819.common.DateUtils.isWorkday;
import static it.unibo.bd1819.common.DateUtils.parseDateFromString;
import static it.unibo.bd1819.common.DateUtils.parseNullableDate;

import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

/** The class models the data contained in questions.csv file. */
public class Question {
    private static final int ID_INDEX = 0;
    private static final int CREATION_DATE_INDEX = 1;
    private static final int CLOSED_DATE_INDEX = 2;
    private static final int DELETION_DATE_INDEX = 3;
    private static final int SCORE_INDEX = 4;
    private static final int OWNER_ID_INDEX = 5;
    private static final String NOT_AVAILABLE = "NA";
    private static final int ANSWER_COUNT_INDEX = 6;
    
    private static final int NUMBER_OF_FIELDS = 7;
    
    private final long id;
    private final DateTime creationDate;
    @Nullable
    private final DateTime closedDate;
    @Nullable
    private final DateTime deletionDate;
    private final int score;
    @Nullable
    private final String ownerUserId;
    private final int answerCount;

    /**
     * Object constructor.
     *
     * @param id           the ID of the question
     * @param creationDate the date the question was created on
     * @param closedDate   the date the question was closed on, or null if it was never closed
     * @param deletionDate the date the question was deleted on, or null if it was never deleted
     * @param score        the score gained by the question
     * @param ownerUserId  the ID of the user that owns (created) the question
     * @param answerCount  the number of answers of the question (must be positive)
     *
     * @throws IllegalArgumentException if data are not 6 elements, dates can't be parsed, or answer count is negative
     * @throws NumberFormatException    if numeric data can't be parsed
     */
    @Contract(pure = true)
    public Question(
        final long id,
        @Nullable final String creationDate, @Nullable final String closedDate, @Nullable final String deletionDate,
        final int score, @Nullable final String ownerUserId, final int answerCount) {
        this.id = id;
        this.creationDate = parseDateFromString(creationDate);
        this.closedDate = parseDateFromString(closedDate);
        this.deletionDate = parseDateFromString(deletionDate);
        this.score = score;
        this.ownerUserId = "NA".equals(ownerUserId) ? null : ownerUserId;
        if (answerCount < 0) {
            throw new IllegalArgumentException("The number of answers can't be negative");
        }
        this.answerCount = answerCount;
    }

    /**
     * Factory method that builds a Question object from Hadoop tuple.
     *
     * @param text the text to parse
     *
     * @return the Question object
     *
     * @throws IllegalArgumentException if data are not 6 elements, dates can't be parsed, or answer count is negative
     * @throws NumberFormatException    if numeric data can't be parsed
     */
    @NotNull
    @Contract("_ -> new")
    public static Question parseText(final Text text) {
        final String[] line = text.toString().split(",");

        if (line.length != NUMBER_OF_FIELDS) {
            throw new IllegalArgumentException("Unexpected line format: columns: " + line.length);
        }

        return new Question(
            Long.parseLong(line[ID_INDEX]),
            line[CREATION_DATE_INDEX],
            parseNullableDate(line[CLOSED_DATE_INDEX]),
            parseNullableDate(line[DELETION_DATE_INDEX]),
            Integer.parseInt(line[SCORE_INDEX]),
            NOT_AVAILABLE.equals(line[OWNER_ID_INDEX]) ? null : line[OWNER_ID_INDEX],
            Integer.parseInt(line[ANSWER_COUNT_INDEX]));
    }

    /**
     * Get the ID of the question.
     *
     * @return the ID
     */
    public long getId() {
        return this.id;
    }

    /**
     * Get the creation date of the question.
     *
     * @return the creation date, or null if
     */
    @NotNull
    public String getCreationDate() {
        return this.creationDate.toString();
    }

    /**
     * Checks if the question was created in a working day.
     *
     * @return true if was created in a workday, false if it was created in a holiday.
     */
    public boolean isCreatedInWorkday() {
        return isWorkday(this.creationDate);
    }

    /**
     * Get the date when the question was closed, if any.
     *
     * @return the date as UTC string, if any, or null otherwise
     */
    @Nullable
    public String getClosedDate() {
        return this.closedDate != null ? this.closedDate.toString() : null;
    }

    /**
     * Get the date when the question was deleted, if any.
     *
     * @return the date as UTC string, if any, or null otherwise
     */
    @Nullable
    public String getDeletionDate() {
        return this.deletionDate != null ? this.deletionDate.toString() : null;
    }

    /**
     * Get the score of the question.
     *
     * @return the score
     */
    public int getScore() {
        return this.score;
    }

    /**
     * Get the user ID of the owner of the question.
     *
     * @return the numeric user ID
     */
    @Nullable
    public String getOwnerUserId() {
        return this.ownerUserId;
    }

    /**
     * Get the number of answers of the question.
     *
     * @return the number of answers of the question
     */
    public int getAnswerCount() {
        return this.answerCount;
    }
}
