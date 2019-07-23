package it.unibo.bd1819.common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

import static it.unibo.bd1819.common.DateUtils.isWorkday;
import static it.unibo.bd1819.common.DateUtils.parseDateFromString;
import static it.unibo.bd1819.common.DateUtils.parseNullableDate;

/** The class models the data contained in questions.csv file. */
public class Question {
    private static final int NUMBER_OF_FIELDS = 7;
    
    private final long id;
    private final DateTime creationDate;
    @Nullable
    private final DateTime closedDate;
    @Nullable
    private final DateTime deletionDate;
    private final int score;
    @Nullable
    private final Integer ownerUserId;
    @Nullable
    private final Integer answerCount;

    @Contract(pure = true)
    public Question(
        final long id,
        @Nullable final String creationDate, @Nullable final String closedDate, @Nullable final String deletionDate,
        final int score, @Nullable final Integer ownerUserId, @Nullable final Integer answerCount) {
        this.id = id;
        this.creationDate = parseDateFromString(creationDate);
        this.closedDate = parseDateFromString(closedDate);
        this.deletionDate = parseDateFromString(deletionDate);
        this.score = score;
        this.ownerUserId = ownerUserId;
        this.answerCount = answerCount;
    }

    /**
     * Factory method that builds a Question object from Hadoop tuple.
     *
     * @param id   the Question ID
     * @param text the Question Data
     *
     * @return the Question object
     *
     * @throws IllegalArgumentException if data are not 6 elements, or dates can't be parsed
     * @throws NumberFormatException    if numeric data can't be parsed
     */
    @NotNull
    @Contract("_, _ -> new")
    public static Question parseText(final LongWritable id, final Text text) {
        final String[] line = text.toString().split(",");

        if (line.length != NUMBER_OF_FIELDS) {
            throw new IllegalArgumentException("Unexpected line format: columns: " + line.length);
        }

        return new Question(
            Long.parseLong(line[0]), line[1], parseNullableDate(line[2]), parseNullableDate(line[3]),
            Integer.parseInt(line[4]), line[5].equals("NA") ? null : Integer.parseInt(line[5]),
            line[6].equals("NA") ? null : Integer.parseInt(line[6]));
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
    public Integer getOwnerUserId() {
        return this.ownerUserId;
    }

    /**
     * Get the number of answers of the question.
     *
     * @return the number of answers of the question
     */
    @Nullable
    public Integer getAnswerCount() {
        return this.answerCount;
    }
}
