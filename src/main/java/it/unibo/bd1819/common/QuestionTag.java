package it.unibo.bd1819.common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/** The class models the data contained in question_tags.csv file. */
public class QuestionTag {
    private final long id;
    private final String tag;

    private QuestionTag(final long id, final String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * Factory method that builds a Question object from Hadoop tuple.
     *
     * @param id  the Question ID
     * @param tag the Question Tag
     *
     * @return the QuestionTag object
     *
     * @throws NumberFormatException if numeric data can't be parsed
     */
    public static QuestionTag parseText(final LongWritable id, final Text tag) {
        return new QuestionTag(id.get(), tag.toString());
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
