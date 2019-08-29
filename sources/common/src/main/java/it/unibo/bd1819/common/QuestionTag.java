package it.unibo.bd1819.common;

import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/** The class models the data contained in question_tags.csv file. */
public class QuestionTag {
    private final long id;
    private final String tag;

    @Contract(pure = true)
    public QuestionTag(final long id, final @NotNull String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * Factory method that builds a Question object from Hadoop tuple.
     *
     * @param text the text to parse
     *
     * @return the QuestionTag object
     *
     * @throws NumberFormatException if numeric data can't be parsed
     */
    @NotNull
    @Contract("_ -> new")
    public static QuestionTag parseText(final Text text) {
        final String[] data = text.toString().split(",");
        return new QuestionTag(Long.parseLong(data[0]), data[1]);
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
     * Get a tag of the question.
     *
     * @return the tag
     */
    public String getTag() {
        return this.tag;
    }
}
